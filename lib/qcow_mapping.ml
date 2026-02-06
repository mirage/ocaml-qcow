type t

external create : int64 -> t = "stub_qcow_mapping_create"

external extend : t -> int64 -> unit = "stub_qcow_mapping_extend"

external get : t -> int64 -> int64 = "stub_qcow_mapping_get"

external set : t -> int64 -> int64 -> unit = "stub_qcow_mapping_set"

external length : t -> int64 = "stub_qcow_mapping_length"

external get_sparse_interval_stub :
  t -> int64 -> int64 -> (int64 * int64 * int64) option
  = "stub_qcow_mapping_get_sparse_interval"

let get_sparse_interval t ~index ~cluster_bits =
  get_sparse_interval_stub t index cluster_bits

let to_interval_seq arr cluster_bits =
  let rec aux arr length index () =
    if index = length then
      Seq.Nil
    else
      match get_sparse_interval arr ~index ~cluster_bits with
      | None ->
          Seq.Nil
      | Some (right_index, left_cluster, right_cluster) ->
          Seq.Cons
            ( (left_cluster, right_cluster)
            , aux arr length (Int64.succ right_index)
            )
  in
  aux arr (length arr) 0L
