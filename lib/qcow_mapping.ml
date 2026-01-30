type t

external create : int64 -> t = "stub_qcow_mapping_create"

external extend : t -> int64 -> unit = "stub_qcow_mapping_extend"

external get : t -> int64 -> int64 = "stub_qcow_mapping_get"

external set : t -> int64 -> int64 -> unit = "stub_qcow_mapping_set"

external length : t -> int64 = "stub_qcow_mapping_length"
