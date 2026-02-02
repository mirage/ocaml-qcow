#include <stdint.h>
#include <string.h>
#include <errno.h>

#define CAML_NAME_SPACE
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/threads.h>
#include <caml/fail.h>
#include <caml/custom.h>

typedef struct array {
    uint64_t length;
    int64_t* a;
} array;

static inline array *
qcow_mapping_arr_of_val (value v)
{
    array *arr = *(array **) Data_custom_val(v);

    return arr;
}

static void
qcow_mapping_finalize (value v)
{
    array *arr = qcow_mapping_arr_of_val (v);
    free(arr->a);
    free(arr);
}

static struct custom_operations qcow_mapping_ops = {
    .identifier = "qcow_mapping_array",
    .finalize = qcow_mapping_finalize,
    .compare = custom_compare_default,      /* Can't compare     */
    .hash = custom_hash_default,    /* Can't hash        */
    .serialize = custom_serialize_default,  /* Can't serialize   */
    .deserialize = custom_deserialize_default,      /* Can't deserialize */
    .compare_ext = custom_compare_ext_default,      /* Can't compare     */
};

#define Arr_val(v) (*((array **) Data_custom_val(v)))

CAMLprim value
stub_qcow_mapping_create (value length_val)
{
    CAMLparam1(length_val);
    CAMLlocal1(result);
    array *arr = malloc(sizeof *arr);

    arr->length = Int64_val(length_val);
    result = caml_alloc_custom(&qcow_mapping_ops, sizeof(array *), 0, 1);

    caml_release_runtime_system();
    arr->a = malloc(sizeof(uint64_t) * arr->length);
    caml_acquire_runtime_system();

    if (!arr->a)
        caml_failwith(strerror(errno));

    caml_release_runtime_system();
    // Initialize to -1, otherwise there's no way to distinguish data clusters
    // from table clusters and empty clusters
    for (size_t i = 0; i < arr->length; i++) {
        arr->a[i] = -1;
    }
    caml_acquire_runtime_system();

    Arr_val(result) = arr;

    CAMLreturn(result);
}

CAMLprim value
stub_qcow_mapping_extend (value t_val, value length_val)
{
    CAMLparam2(t_val, length_val);
    CAMLlocal1(result);
    array* arr = qcow_mapping_arr_of_val(t_val);

    uint64_t new_length = Int64_val(length_val);

    caml_release_runtime_system();
    arr->a = realloc(arr->a, sizeof(uint64_t) * new_length);
    caml_acquire_runtime_system();

    if (!arr->a)
        caml_failwith(strerror(errno));

    caml_release_runtime_system();
    // Initialize the newly allocated cells to -1, otherwise there's no way
    // to distinguish data clusters from table clusters and empty clusters
    for (size_t i = arr->length; i < new_length; i++) {
        arr->a[i] = -1;
    }
    arr->length = new_length;
    caml_acquire_runtime_system();

    CAMLreturn(Val_unit);
}

CAMLprim value
stub_qcow_mapping_get (value t_val, value index_val)
{
    CAMLparam2(t_val, index_val);
    CAMLlocal1(result);
    array* arr = qcow_mapping_arr_of_val(t_val);

    uint64_t index = Int64_val(index_val);
    result = caml_copy_int64(arr->a[index]);

    CAMLreturn(result);
}

CAMLprim value
stub_qcow_mapping_set (value t_val, value index_val, value new_val)
{
    CAMLparam3(t_val, index_val, new_val);
    array* arr = qcow_mapping_arr_of_val(t_val);

    uint64_t index = Int64_val(index_val);
    int64_t new = Int64_val(new_val);
    arr->a[index] = new;

    CAMLreturn(Val_unit);
}

CAMLprim value
stub_qcow_mapping_length (value t_val)
{
    CAMLparam1(t_val);
    CAMLlocal1(result);

    array *arr = qcow_mapping_arr_of_val(t_val);
    result = caml_copy_int64(arr->length);

    CAMLreturn(result);
}


CAMLprim value
stub_qcow_mapping_get_sparse_interval (value t_val, value index_val, value cluster_bits_val)
{
    CAMLparam3(t_val, index_val, cluster_bits_val);
    CAMLlocal1(result);
    result = caml_alloc_tuple(3);

    array* arr = qcow_mapping_arr_of_val(t_val);

    uint64_t cluster_bits = Int64_val(cluster_bits_val);
    uint64_t diff_bw_next_clusters = 1 << cluster_bits;
    uint64_t left_index = Int64_val(index_val);
    uint64_t right_index = left_index;

    // Find the longest interval of subsequent allocated data clusters
    // (data clusters are subsequent if they're located right next to each
    // other on the virtual disk), return the index to be used in the next
    // search iteration and the [interval_start, interval_end] pair of virtual
    // clusters (both inclusive)
    for (size_t i = left_index+1; i < arr->length; i++, right_index++) {
        if (arr->a[i-1] == -1) {
            if (arr->a[i] == -1 && i == (arr->length-1)) {
                CAMLreturn(Val_none);
            }
            left_index++;
        } else if (arr->a[i-1] + diff_bw_next_clusters != arr->a[i]) {
            break;
        }
    }

    Store_field(result, 0, caml_copy_int64(right_index));
    Store_field(result, 1, caml_copy_int64((arr->a[left_index]) >> cluster_bits));
    Store_field(result, 2, caml_copy_int64((arr->a[right_index]) >> cluster_bits));

    CAMLreturn(caml_alloc_some(result));
}
