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
