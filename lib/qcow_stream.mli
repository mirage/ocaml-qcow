module Header = Qcow_header
module Cache = Qcow_cache
open Qcow_types

exception Reference_outside_file of int64 * int64

val start_stream_decode :
  Lwt_unix.file_descr -> (int64 * int32 * int64 ref * int64 Cluster.Map.t) Lwt.t
(** Decodes QCOW header and tables from the beginning of the stream,
    constructing a map of data clusters.
    Returns (virtual_size, cluster_bits, last_read_cluster, data_cluster_map)
   *)

val decode_header :
  Unix.file_descr -> int64 * int32 * int64 ref * int64 Cluster.Map.t
(** Wraps [start_stream_decode] in an Lwt context *)

val copy_data :
     progress_cb:(int -> unit)
  -> int64 ref
  -> int32
  -> Lwt_unix.file_descr
  -> Lwt_unix.file_descr
  -> int64 Cluster.Map.t
  -> unit Lwt.t
(** [copy_data last_read_cluster cluster_bits input_fd output_fd data_cluster_map]
    Copies data cluster-by-cluster concurrently *)

val stream_decode :
     ?progress_cb:(int -> unit)
  -> ?header_info:int64 * int32 * int64 ref * int64 Cluster.Map.t
  -> Unix.file_descr
  -> string
  -> unit
(** [stream_decode ?progress_cb ?header_info fd output_path] decodes the input
    QCOW stream from [fd], writing out the raw file to [output_path].

    Accepts [header_info] returned by [start_stream_decode] and [decode_header]
    if the header has already been parsed separately (to determine the virtual
    size, for example).

    If provided, [progress_cb] is a callback that will be called with integer
    percentage values representing copying progress.
    *)
