(*
 * Copyright (C) 2015 David Scott <dave@recoil.org>
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

(** Common signatures used by the library *)

open Result

module type LOG = sig
  (** Common logging functions *)

  val debug : ('a, unit, string, unit) format4 -> 'a
  val info  : ('a, unit, string, unit) format4 -> 'a
  val error : ('a, unit, string, unit) format4 -> 'a
end

module type SERIALISABLE = sig
  (** Values which can be read and written *)

  type t
  (** Instances of this type can be read and written *)

  val sizeof: t -> int
  (** The size of a buffer needed to hold [t] *)

  val read: Cstruct.t -> (t * Cstruct.t, [ `Msg of string]) result
  (** Read a [t] from the given buffer and return it, along with the
      unused remainder of the buffer. If the buffer cannot
      be parsed then return an error.*)

  val write: t -> Cstruct.t -> (Cstruct.t, [ `Msg of string]) result
  (** Write a [t] into the given buffer. If the buffer is too small,
      then return an error. Return the unused remainder of the buffer.*)
end

module type PRINTABLE = sig
  (** Values which can be pretty-printed *)

  type t
  (** Instances of this type can be pretty-printed *)

  val to_string: t -> string
  (** Produce a pretty human-readable string from a value *)
end

module type RESIZABLE_BLOCK = sig
  include V1_LWT.BLOCK

  val resize: t -> int64 -> [ `Ok of unit | `Error of error ] Lwt.t
  (** Resize the file to the given number of sectors. *)

  val flush : t -> [ `Ok of unit | `Error of error ] io
  (** [flush t] flushes any buffers, if the file has been opened in buffered
      mode *)
end

module type DEBUG = sig
  type t
  type error

  val check_no_overlaps: t -> [ `Ok of unit | `Error of error ] Lwt.t

  val set_next_cluster: t -> int64 -> unit
end
