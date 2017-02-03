(*
 * Copyright (C) 2017 Docker Inc
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

module Make(B: Qcow_s.RESIZABLE_BLOCK): sig
  type t
  (** A cluster recycling engine *)

  val create: base:B.t -> sector_size:int -> cluster_bits:int
    -> cache:Qcow_cache.t -> locks:Qcow_cluster.t
    -> metadata:Qcow_metadata.t -> t
  (** Initialise a cluster recycler over the given block device *)

  val set_cluster_map: t -> Qcow_cluster_map.t -> unit
  (** Set the associated cluster map (which will be updated on every cluster
      write) *)

  val reset: t -> unit
  (** Drop all state: useful when some other function has made untracked
      changes to the used/free blocks. Eventually this probably should be
      removed the compact function should be rewritten *)

  val add_to_junk: t -> int64 -> unit
  (** Input the given cluster (with arbitrary contents) to the recycler *)

  val allocate: t -> int64 -> Qcow_clusterset.t option
  (** [allocate t n] returns [n] clusters which are ready for re-use. If there
      are not enough clusters free then this returns None. *)

  val erase: t -> Qcow_clusterset.t -> (unit, B.write_error) result Lwt.t
  (** Write zeroes over the specified set of clusters *)

  val copy: t -> int64 -> int64 -> (unit, B.write_error) result Lwt.t
  (** [copy src dst] copies the cluster [src] to [dst] *)

  val move: t -> Qcow_cluster_map.Move.t -> (unit, B.write_error) result Lwt.t
  (** [move t mv] perform the initial data copy of the move operation [mv] *)

  val update_references: t -> (unit, Qcow_metadata.write_error) result Lwt.t
  (** [update_references t] rewrites references to any recently copied and
      flushed block. *)

  val erase_all: t -> (unit, B.write_error) result Lwt.t
  (** Erase all junk clusters *)

  val flush: t -> (unit, B.write_error) result Lwt.t
  (** Issue a flush to the block device, update internal recycler state. *)
end