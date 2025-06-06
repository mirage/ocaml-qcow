(*
 * Copyright (C) 2017 David Scott <dave@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)
open Qcow_types

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info) ;
  src

module Log = (val Logs.src_log src : Logs.LOG)

type t = {
    read_cluster: Cluster.t -> (Cstruct.t, Mirage_block.error) result Lwt.t
  ; write_cluster:
      Cluster.t -> Cstruct.t -> (unit, Mirage_block.write_error) result Lwt.t
  ; mutable clusters: Cstruct.t Cluster.Map.t
  ; seekable: bool
  ; last_read_cluster: Cluster.t ref
}

let create ~read_cluster ~write_cluster ?(seekable = true) () =
  let clusters = Cluster.Map.empty in
  {
    read_cluster
  ; write_cluster
  ; clusters
  ; seekable
  ; last_read_cluster= ref (Cluster.of_int 0)
  }

let read t cluster =
  if Cluster.Map.mem cluster t.clusters then
    let data = Cluster.Map.find cluster t.clusters in
    Lwt.return (Ok data)
  else
    let open Lwt.Infix in
    let read_cluster cluster =
      t.read_cluster cluster >>= function
      | Error e ->
          Lwt.return (Error e)
      | Ok data ->
          t.clusters <- Cluster.Map.add cluster data t.clusters ;
          Lwt.return (Ok data)
    in
    let next_cluster = Cluster.succ !(t.last_read_cluster) in
    if t.seekable then
      read_cluster cluster
    else
      (* If we can't seek, we need to read sequential clusters until we reach
         the one we want. Previous clusters will still be stored in the cache
         for when we need them later (since we can't seek back) *)
      let rec read_clusters ~from ~until =
        let data = read_cluster from in
        t.last_read_cluster := from ;
        if from < until then
          read_clusters ~from:(Cluster.succ from) ~until
        else
          data
      in
      read_clusters ~from:next_cluster ~until:cluster

let write t cluster data =
  if not (Cluster.Map.mem cluster t.clusters) then (
    Log.err (fun f ->
        f
          "Cache.write %s: cluster is nolonger in cache, so update will be \
           dropped"
          (Cluster.to_string cluster)
    ) ;
    assert false
  ) ;
  t.clusters <- Cluster.Map.add cluster data t.clusters ;
  t.write_cluster cluster data

let remove t cluster =
  if Cluster.Map.mem cluster t.clusters then
    Printf.fprintf stderr "Dropping cache for cluster %s\n"
      (Cluster.to_string cluster) ;
  t.clusters <- Cluster.Map.remove cluster t.clusters

let resize t new_size_clusters =
  let to_keep, to_drop =
    Cluster.Map.partition
      (fun cluster _ -> cluster < new_size_clusters)
      t.clusters
  in
  t.clusters <- to_keep ;
  if not (Cluster.Map.is_empty to_drop) then
    Log.info (fun f ->
        f "After file resize dropping cached clusters: %s"
          (String.concat ", "
          @@ List.map Cluster.to_string
          @@ List.map fst
          @@ Cluster.Map.bindings to_drop
          )
    )

module Debug = struct
  let assert_not_cached t cluster =
    if Cluster.Map.mem cluster t.clusters then (
      Printf.fprintf stderr "Cluster %s still in the metadata cache\n"
        (Cluster.to_string cluster) ;
      assert false
    )

  let all_cached_clusters t =
    Cluster.Map.fold
      (fun cluster _ set ->
        Cluster.IntervalSet.(add (Interval.make cluster cluster) set)
      )
      t.clusters Cluster.IntervalSet.empty

  let check_disk t =
    let open Lwt.Infix in
    let rec loop = function
      | [] ->
          Lwt.return (Ok ())
      | (cluster, expected) :: rest -> (
          (t.read_cluster cluster >>= function
           | Error e ->
               Lwt.return (Error e)
           | Ok data ->
               if not (Cstruct.equal expected data) then (
                 Log.err (fun f ->
                     f "Cache for cluster %s disagrees with disk"
                       (Cluster.to_string cluster)
                 ) ;
                 Log.err (fun f -> f "Cached:") ;
                 let buffer = Buffer.create 65536 in
                 Cstruct.hexdump_to_buffer buffer expected ;
                 Log.err (fun f -> f "%s" (Buffer.contents buffer)) ;
                 let buffer = Buffer.create 65536 in
                 Cstruct.hexdump_to_buffer buffer data ;
                 Log.err (fun f -> f "On disk:") ;
                 Log.err (fun f -> f "%s" (Buffer.contents buffer)) ;
                 Lwt.return (Ok ())
               ) else
                 Lwt.return (Ok ())
          )
          >>= function
          | Error e ->
              Lwt.return (Error e)
          | Ok () ->
              loop rest
        )
    in
    loop (Cluster.Map.bindings t.clusters)
end
