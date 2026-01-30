type t

val create : int64 -> t

val extend : t -> int64 -> unit

val get : t -> int64 -> int64

val set : t -> int64 -> int64 -> unit

val length : t -> int64
