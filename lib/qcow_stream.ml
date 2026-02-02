(* A modified subset of qcow.ml for the streaming use case, when seeking in the
   input file is not possible. Avoids the usage of some Mirage libraries that
   assume seeking is available and in general does only the least amount of
   work required, dropping some unnecessary code *)

open Qcow_types
module Error = Qcow_error
module Header = Qcow_header
module Virtual = Qcow_virtual
module Physical = Qcow_physical
module Locks = Qcow_locks
module Int = Qcow_int
module Int64 = Qcow_types.Int64
module Lwt_error = Qcow_error.Lwt_error
module Cache = Qcow_cache
module Config = Qcow_config
module Metadata = Qcow_metadata

let ( <| ) = Int64.shift_left

let ( -- ) = Int64.sub

let ( ++ ) = Int64.add

let ( // ) = Int64.div

let src =
  let src =
    Logs.Src.create "qcow-stream" ~doc:"qcow2 with streaming capabilities"
  in
  Logs.Src.set_level src (Some Logs.Info) ;
  src

module Log = (val Logs.src_log src : Logs.LOG)

type cluster_info = {i_cluster_bits: int; i_sectors_per_cluster: int64}

(* I/O functions *)

(* Like Lwt_csruct.complete, but does not raise End_of_file, instead returns
   the part of the Cstruct that was read up to EOF *)
let read_complete op t =
  let open Lwt in
  let open Lwt.Syntax in
  let rec loop t bytes_read =
    let* n = op t in
    let t = Cstruct.shift t n in
    if Cstruct.length t = 0 then
      return (bytes_read + n)
    else if n = 0 then
      return bytes_read
    else
      loop t (bytes_read + n)
  in
  let* bytes_read = loop t 0 in
  return (Cstruct.sub t 0 bytes_read)

let stream_read fd buf = read_complete (Lwt_cstruct.read fd) buf

let complete_pwrite_bytes fd buf file_offset =
  let pwrite fd buf ~file_offset ~buf_offset ~len =
    Lwt_unix.pwrite fd buf ~file_offset buf_offset len
  in
  let open Lwt.Syntax in
  let open Lwt in
  let rec loop buf file_offset buf_offset len =
    let* wrote_bytes_nr = pwrite fd buf ~file_offset ~buf_offset ~len in
    Log.debug (fun f -> f "wrote %d bytes, len left %d\n" wrote_bytes_nr len) ;
    if wrote_bytes_nr = len then
      return ()
    else if wrote_bytes_nr = 0 then
      fail End_of_file
    else
      loop buf
        (file_offset + wrote_bytes_nr)
        (buf_offset + wrote_bytes_nr)
        (len - wrote_bytes_nr)
  in
  loop buf file_offset 0 (Bytes.length buf)

let malloc_bytes cluster_bits =
  let cluster_bits = Int32.to_int cluster_bits in
  let size = 1 lsl cluster_bits in
  Bytes.create size

let malloc cluster_bits =
  let cluster_bits = Int32.to_int cluster_bits in
  let npages = max 1 (1 lsl (cluster_bits - 12)) in
  let pages = Io_page.(to_cstruct (get npages)) in
  Cstruct.sub pages 0 (1 lsl cluster_bits)

(* Reads clusters sequentially.
   Since we can't seek in the input file, we can only ever read the next
   cluster (or previous ones if they were cached). qcow2-to-stdout script
   produces nice QCOW files, where the order necessarily is:
     header
     refcount table
     L1 table
     L2 table
     data clusters
   So we do not expect L2 clusters to be interleaved with data clusters - we
   should be able to get a complete picture of all data clusters before reading
   them.

   NOTE: If, in the future, it is desirable to make this more advanced and
   consume QCOW2 files that are not ordered as above, the logic would need to
   be more dynamic.
*)
let read_cluster last_read_cluster fd cluster_bits alloc_func read_func i =
  let cluster = Cluster.to_int64 i in
  if !last_read_cluster ++ 1L = cluster then (
    last_read_cluster := cluster ;
    let buf = alloc_func cluster_bits in
    Log.debug (fun f -> f "\tread_cluster %Lu\n" cluster) ;
    let open Lwt.Infix in
    Lwt.catch
      (fun () -> read_func fd buf >>= fun read_buf -> Lwt.return (Ok read_buf))
      (fun e ->
        Log.err (fun f ->
            f "read_cluster %Ld: low-level I/O exception %s" cluster
              (Printexc.to_string e)
        ) ;
        Lwt.fail e
      )
  ) else
    Lwt.fail_with
      (Printf.sprintf
         "read_cluster_stream: can't read non-sequential clusters \
          (last_read_cluster:%Lu, i: %Lu)"
         !last_read_cluster cluster
      )

exception Reference_outside_file of int64 * int64

exception Compressed_unsupported

(* Reads and parses refcount, L1, L2 tables.
   See the note above on the structure of the QCOW file we expect.
*)
let stream_make_cluster_map h size_sectors cluster_info metadata () =
  let open Lwt_error.Infix in
  let open Lwt.Syntax in
  let cluster_bits, sectors_per_cluster =
    match cluster_info with
    | {i_cluster_bits; i_sectors_per_cluster} ->
        (i_cluster_bits, i_sectors_per_cluster)
  in

  let refcount_start_cluster =
    Cluster.to_int64
    @@ Physical.cluster ~cluster_bits h.Header.refcount_table_offset
  in
  let int64s_per_cluster = 1L <| cluster_bits - 3 in
  let l1_table_start_cluster =
    Cluster.to_int64 @@ Physical.cluster ~cluster_bits h.Header.l1_table_offset
  in
  let l1_table_clusters =
    Int64.(round_up (of_int32 h.Header.l1_size) int64s_per_cluster)
    // int64s_per_cluster
  in

  (* As opposed to make_cluster_map, where size_sectors comes from known
     physical file size, when streaming we do not yet know the full size of
     the file. We instead calculate it as virtual_size + l1 table clusters +
     l2 table clusters + refcount table clusters as we go, and hence
     max_cluster will change accordingly *)
  let max_cluster =
    ref (Cluster.of_int64 (size_sectors // sectors_per_cluster))
  in
  let refcount_table_clusters =
    Int64.of_int32 h.Header.refcount_table_clusters
  in
  max_cluster :=
    Cluster.add !max_cluster
      (Cluster.of_int64 (Int64.of_int32 h.Header.refcount_table_clusters)) ;
  (* There can be a gap between refcount table clusters and L1 clusters for
     some reason, count that against max_cluster as well *)
  let gap =
    l1_table_start_cluster -- (refcount_start_cluster ++ refcount_table_clusters)
  in
  max_cluster := Cluster.add !max_cluster (Cluster.of_int64 gap) ;

  Log.debug (fun f ->
      f
        "refcount_table_clusters is %d\n\
        \ max_cluster is %Lu (virtual size + refcount + gap between refcount \
         and L1 table, to be adjusted)\n\
        \ sectors_per_cluster is %Lu\n"
        (Int32.to_int h.Header.refcount_table_clusters)
        (Cluster.to_int64 !max_cluster)
        sectors_per_cluster
  ) ;
  (* Construct a map of virtual clusters to physical offsets *)
  let data_refs = ref Cluster.Map.empty in

  let parse x =
    if x = Physical.unmapped then
      Cluster.zero
    else if Physical.is_compressed x then (
      (* TODO: Is it worth supporting compressed cluster descriptors? Quite a lot
         of popular in-the-wild images feature these. If it's possible to convert
         an image to get rid of compressed cluster descriptors, note it in the error *)
      Log.err (fun f ->
          f "Unsupported compressed Cluster Descriptor has been found"
      ) ;
      raise Compressed_unsupported
    ) else
      Physical.cluster ~cluster_bits x
  in

  let mark rf cluster is_table =
    let c, w = rf in
    if cluster > !max_cluster then (
      Log.err (fun f ->
          f
            "Found a reference to cluster %s outside the file (max cluster %s) \
             from cluster %s.%d\n"
            (Cluster.to_string cluster)
            (Cluster.to_string !max_cluster)
            (Cluster.to_string c) w
      ) ;
      let src =
        Int64.of_int w
        ++ (Cluster.to_int64 c <| Int32.to_int h.Header.cluster_bits)
      in
      let dst =
        Cluster.to_int64 cluster <| Int32.to_int h.Header.cluster_bits
      in
      raise (Reference_outside_file (src, dst))
    ) ;
    if cluster = Cluster.zero then
      ()
    else if
      (* See note above, we need to account for table clusters when streaming
         since we don't know the physical size of the file *)
      is_table
    then
      max_cluster := Cluster.(add !max_cluster (of_int64 1L))
  in

  (* scan the refcount table *)
  let rec refcount_iter i =
    if i >= Int64.of_int32 h.Header.refcount_table_clusters then
      Lwt.return (Ok ())
    else
      let refcount_cluster =
        Cluster.of_int64 @@ (refcount_start_cluster ++ i)
      in
      Log.debug (fun f ->
          f "reading refcount table in cluster %Lu\n"
            (Cluster.to_int64 refcount_cluster)
      ) ;
      Metadata.read metadata refcount_cluster (fun c ->
          let addresses = Metadata.Physical.of_contents c in
          let rec loop i =
            if i >= Metadata.Physical.len addresses then
              Lwt.return (Ok ())
            else
              let cluster = parse (Metadata.Physical.get addresses i) in
              (* Refcount table clusters were already counted against
                 max_cluster above as their number is known from the header
              *)
              mark (refcount_cluster, i) cluster false ;
              loop (i + 1)
          in
          loop 0
      )
      >>= fun () ->
      let* () = Metadata.remove_from_cache metadata refcount_cluster in
      refcount_iter (Int64.succ i)
  in

  (* construct the map of data clusters *)
  let rec data_iter l1_index l2 l2_table_cluster i =
    let l2_index = Int64.of_int i in
    (* index in the L2 table *)
    if i >= Metadata.Physical.len l2 then
      Lwt.return (Ok ())
    else
      let cluster = parse (Metadata.Physical.get l2 i) in
      (* Data clusters are already counted in virtual file size so
         don't need to be added to max_cluster *)
      mark (l2_table_cluster, i) cluster false ;

      ( if cluster <> Cluster.zero then
          let virt_address = Virtual.{l1_index; l2_index; cluster= 0L} in
          let virt_address = Virtual.to_offset ~cluster_bits virt_address in
          data_refs := Cluster.Map.add cluster virt_address !data_refs
      ) ;
      data_iter l1_index l2 l2_table_cluster (i + 1)
  in

  (* iterate over pointers to L2 clusters *)
  let rec l2_iter l1 l1_table_cluster i =
    if i >= Metadata.Physical.len l1 then
      Lwt.return (Ok ())
    else
      let l1_index = Int64.of_int i in
      (* index in the L1 table *)
      let l2_table_cluster = parse (Metadata.Physical.get l1 i) in
      if l2_table_cluster <> Cluster.zero then (
        Log.debug (fun f ->
            f "reading l2 table in cluster %Lu\n"
              (Cluster.to_int64 l2_table_cluster)
        ) ;
        (* Count L2 table clusters against max_cluster *)
        mark (l1_table_cluster, i) l2_table_cluster true ;
        Metadata.read metadata l2_table_cluster (fun c ->
            let l2 = Metadata.Physical.of_contents c in
            Lwt.return (Ok l2)
        )
        >>= fun l2 ->
        data_iter l1_index l2 l2_table_cluster 0 >>= fun () ->
        let* () = Metadata.remove_from_cache metadata l2_table_cluster in
        l2_iter l1 l1_table_cluster (i + 1)
      ) else
        l2_iter l1 l1_table_cluster (i + 1)
  in

  refcount_iter 0L >>= fun () ->
  (* scan the L1 and L2 tables, marking the L2 and data clusters *)
  let rec l1_iter i =
    let l1_table_cluster = Cluster.of_int64 @@ (l1_table_start_cluster ++ i) in
    Log.debug (fun f ->
        f "reading l1 table in cluster %Lu\n" (Cluster.to_int64 l1_table_cluster)
    ) ;
    if i >= l1_table_clusters then
      Lwt.return (Ok ())
    else
      Metadata.read metadata l1_table_cluster (fun c ->
          let l1 = Metadata.Physical.of_contents c in
          Lwt.return (Ok l1)
      )
      >>= fun l1 ->
      (* Count L1 table clusters against max_cluster *)
      (max_cluster := Cluster.(add !max_cluster (of_int64 1L))) ;
      l2_iter l1 l1_table_cluster 0 >>= fun () ->
      let* () = Metadata.remove_from_cache metadata l1_table_cluster in
      l1_iter (Int64.succ i)
  in
  l1_iter 0L >>= fun () -> Lwt.return (Ok !data_refs)

let stream_make last_read_cluster fd h sector_size =
  (* The virtual disk has 512 byte sectors *)
  let size_sectors = h.Header.size // 512L in
  let cluster_bits = Int32.to_int h.Header.cluster_bits in
  let cluster_size = 1L <| cluster_bits in
  let sectors_per_cluster = cluster_size // Int64.of_int sector_size in
  Log.debug (fun f ->
      f "size: %Lu\n cluster_size: %Lu\n size_sectors: %Lu\n size_sector: %d\n"
        h.Header.size cluster_size size_sectors sector_size
  ) ;

  let locks = Locks.make () in
  let read_cluster =
    read_cluster last_read_cluster fd h.cluster_bits malloc stream_read
  in
  let write_cluster _i _buf = assert false in
  let cache = Cache.create ~read_cluster ~write_cluster ~seekable:false () in
  let metadata = Metadata.make ~cache ~cluster_bits ~locks () in
  let cluster_info =
    {i_cluster_bits= cluster_bits; i_sectors_per_cluster= sectors_per_cluster}
  in
  Lwt_error.or_fail_with
  @@ stream_make_cluster_map h size_sectors cluster_info metadata ()

let start_stream_decode fd =
  let open Lwt.Syntax in
  (* Read a single sector from the beginning of the stream *)
  let sector_size = 512 in
  let buf = Cstruct.sub Io_page.(to_cstruct (get 1)) 0 sector_size in
  let* buf = stream_read fd buf in
  (* Parse the header *)
  match Qcow_header.read buf with
  | Error (`Msg msg) ->
      Lwt.fail_with msg
  | Ok (header, _rem) ->
      (* Read to the end of the first cluster so that further reads
         start at the cluster boundary.
         First cluster only contains the header: "If the image has a backing
         file then the backing file name should be stored in the remaining
         space between the end of the header extension area and the end of
         the first cluster. It is not allowed to store other data here" *)
      let cluster_bits = Int32.to_int header.cluster_bits in
      let cluster_size = 1 lsl cluster_bits in
      let npages = 1 lsl (cluster_bits - 12) in
      Log.debug (fun f -> f "pages_left_to_read is %d\n" npages) ;
      ( if npages > 0 then
          let pages = Io_page.(to_cstruct (get npages)) in
          (* We've already read a single 512-byte sector *)
          let buf = Cstruct.sub pages 0 (cluster_size - 512) in
          Lwt.ignore_result (stream_read fd buf)
      ) ;

      (* Parse all the tables to get a full map of data clusters *)
      let last_read_cluster = ref 0L in
      let* data_cluster_map =
        stream_make last_read_cluster fd header sector_size
      in
      Lwt.return
        ( header.Header.size
        , header.cluster_bits
        , last_read_cluster
        , data_cluster_map
        )

let copy_data ~progress_cb last_read_cluster cluster_bits input_fd output_fd
    data_cluster_map =
  let open Lwt.Syntax in
  let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input input_fd in
  let complete_read_bytes ic buf =
    let* () = Lwt_io.read_into_exactly ic buf 0 (Bytes.length buf) in
    Lwt.return buf
  in
  let read_cluster_bytes =
    read_cluster last_read_cluster input_channel cluster_bits malloc_bytes
      complete_read_bytes
  in

  (* We'll run multiple threads to try to overlap writes.
     We can't overlap reads since it's a nonseekable stream, so lock them
     behind a mutex *)
  let read_mutex = Lwt_mutex.create () in

  match Cluster.Map.max_binding_opt data_cluster_map with
  | Some (max_cluster, _) ->
      let cur_percent = ref 0 in
      let thread (cluster, file_offset) =
        (* Copy the entire cluster *)
        Log.debug (fun f ->
            f "copy cluster: %Lu, file_offset : %Lu\n"
              (Cluster.to_int64 cluster) file_offset
        ) ;
        let now_percent =
          Cluster.(to_int cluster / (to_int max_cluster * 100))
        in
        if now_percent > !cur_percent then (
          cur_percent := now_percent ;
          progress_cb now_percent
        ) ;
        (* NOTE: no other Lwt promise can be called between the start of the thread
           and the mutex locking or the order of reads would be disrupted.
           Threads are woken up in the order they locked the mutex, so the order is
           currently preserved.
        *)
        let* buf =
          Lwt_mutex.with_lock read_mutex (fun () -> read_cluster_bytes cluster)
        in
        match buf with
        | Ok buf ->
            complete_pwrite_bytes output_fd buf (Int64.to_int file_offset)
        | Error _ ->
            failwith "I/O error"
      in
      let seq = Cluster.Map.to_seq data_cluster_map in
      let seq = Lwt_seq.of_seq seq in
      Lwt_seq.iter_n ~max_concurrency:8 thread seq
  | None ->
      Lwt.return_unit

let stream_decode ?(progress_cb = fun _x -> ()) ?header_info input_fd
    output_path =
  let open Lwt.Syntax in
  let input_fd = Lwt_unix.of_unix_file_descr input_fd in
  let t =
    let* virtual_size, cluster_bits, last_read_cluster, data_cluster_map =
      match header_info with
      | None ->
          start_stream_decode input_fd
      | Some x ->
          Lwt.return x
    in

    let* output_fd =
      Lwt_unix.openfile output_path [Lwt_unix.O_WRONLY; Lwt_unix.O_CREAT] 0o0644
    in
    (* NOTE: We can't ftruncate on a block device, so check if the output file
       is a regular file first *)
    let* output_file_stats = Lwt_unix.LargeFile.fstat output_fd in
    let output_file_kind = output_file_stats.Lwt_unix.LargeFile.st_kind in
    let* () =
      if output_file_kind = Lwt_unix.S_REG then
        Lwt_unix.LargeFile.ftruncate output_fd virtual_size
      else
        Lwt.return_unit
    in

    let* () =
      copy_data ~progress_cb last_read_cluster cluster_bits input_fd output_fd
        data_cluster_map
    in

    let* () = Lwt_unix.close output_fd in
    Lwt.return_unit
  in

  Lwt_main.run t ; ()
