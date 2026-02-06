type t

val create : int64 -> t

val extend : t -> int64 -> unit

val get : t -> int64 -> int64

val set : t -> int64 -> int64 -> unit

val length : t -> int64

(** [to_interval_seq t cluster_bits] returns a sequence of allocated virtual
    data cluster intervals, intended to be used with sparse disks.

    Thus, for a physical image represented by an array of physical clusters
    and their corresponding virtual clusters:
        [-1; 1; 2; 3; 7; -1; 4]

    [to_interval_seq] will return:
        (1,3); (7,7); (4,4)
    *)
val to_interval_seq : t -> int64 -> (int64 * int64) Seq.t
