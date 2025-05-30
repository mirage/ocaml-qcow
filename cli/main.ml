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
open Astring

let project_url = "http://github.com/mirage/ocaml-qcow"

open Cmdliner

(* Help sections common to all commands *)

let _common_options = "COMMON OPTIONS"

let help =
  [
    `S _common_options
  ; `P "These options are common to all commands."
  ; `S "MORE HELP"
  ; `P "Use `$(mname) $(i,COMMAND) --help' for help on a single command."
  ; `Noblank
  ; `S "BUGS"
  ; `P (Printf.sprintf "Check bug reports at %s" project_url)
  ]

(* Options common to all commands *)
let common_options_t =
  let docs = _common_options in
  let debug =
    let doc = "Give only debug output." in
    Arg.(value & flag & info ["debug"] ~docs ~doc)
  in
  let progress =
    let doc = "Display a progress bar." in
    Arg.(value & flag & info ["progress"] ~docs ~doc)
  in
  let progress_fd =
    let doc = "Write machine-readable progress output." in
    Arg.(value & opt (some int) None & info ["progress-fd"] ~docs ~doc)
  in
  Term.(const Common.make $ debug $ progress $ progress_fd)

let filename default =
  let doc = Printf.sprintf "Path to the qcow2 file." in
  Arg.(value & pos 0 file default & info [] ~doc)

let kib = 1024L

let mib = Int64.mul kib 1024L

let gib = Int64.mul mib 1024L

let tib = Int64.mul gib 1024L

let pib = Int64.mul tib 1024L

let sizes =
  List.sort
    (fun (_, a) (_, b) -> compare a b)
    [("KiB", kib); ("MiB", mib); ("GiB", gib); ("TiB", tib); ("PiB", pib)]

let size_parser txt =
  let prefix suffix txt =
    let suffix' = String.length suffix in
    let txt' = String.length txt in
    String.with_range ~len:(txt' - suffix') txt
  in
  try
    match
      List.fold_left
        (fun acc (suffix, multiplier) ->
          match acc with
          | Some x ->
              Some x
          | None when not (String.is_suffix ~affix:suffix txt) ->
              None
          | None ->
              Some Int64.(mul multiplier (of_string (prefix suffix txt)))
        )
        None sizes
    with
    | None ->
        `Ok (Int64.of_string txt)
    | Some x ->
        `Ok x
  with Failure _ -> `Error ("invalid size: " ^ txt)

let size_printer ppf v =
  let txt =
    match
      List.fold_left
        (fun acc (suffix, multiplier) ->
          match acc with
          | Some x ->
              Some x
          | None when Int64.rem v multiplier = 0L ->
              Some Int64.(to_string (div v multiplier) ^ suffix)
          | None ->
              None
        )
        None sizes
    with
    | None ->
        Int64.to_string v
    | Some x ->
        x
  in
  Format.fprintf ppf "%s" txt

let size_converter = (size_parser, size_printer)

let size =
  let doc = Printf.sprintf "Virtual size of the qcow image" in
  Arg.(value & opt size_converter 1024L & info ["size"] ~doc)

let output_parser txt =
  match String.Ascii.lowercase txt with
  | "text" ->
      `Ok `Text
  | "json" ->
      `Ok `Json
  | _ ->
      `Error "Unknown output format, expected either 'text' or 'json'"

let output_printer ppf v =
  Format.fprintf ppf "%s" (match v with `Text -> "text" | `Json -> "json")

let output_converter = (output_parser, output_printer)

let output_format =
  let doc = "Desired output format" in
  Arg.(value & opt output_converter `Text & info ["output"] ~doc)

let strict_refcounts =
  let doc = Printf.sprintf "Use strict (non-lazy) refcounts" in
  Arg.(value & flag & info ["strict-refcounts"] ~doc)

let output default =
  let doc = Printf.sprintf "Path to the output file." in
  Arg.(value & pos 1 string default & info [] ~doc)

let trace =
  let doc = Printf.sprintf "Print block device accesses for debugging" in
  Arg.(value & flag & info ["trace"] ~doc)

let ignore_data_loss =
  let doc =
    Printf.sprintf
      "Ignore potential data loss and proceed anyway. Use with extreme caution!"
  in
  Arg.(value & flag & info ["ignore-data-loss"] ~doc)

let ignore_zeroes =
  let doc = "Scan for and ignore blocks which are full of zeroes" in
  Arg.(value & flag & info ["ignore-zeroes"] ~doc)

let filter =
  let doc = "Path within the structure" in
  Arg.(value & opt (some string) None & info ["filter"] ~doc)

let info_cmd =
  let doc = "display general information about a qcow2" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P "Display the contents of a qcow2 file header."
    ; `P
        "By default the full header is printed as an s-expression. To print \
         only some fields provide a --filter argument."
    ; `S "EXAMPLES"
    ; `P "To print the file size:"
    ; `P "$(mname) info <filename> --filter .size"
    ; `P "To print the dirty flag:"
    ; `P "$(mname) info <filename> --filter .additional.[0].dirty"
    ]
    @ help
  in
  ( Term.(ret (const Impl.info $ filename "test.qcow2" $ filter))
  , Cmd.info "info" ~sdocs:_common_options ~doc ~man
  )

let check_cmd =
  let doc = "check the device for internal consistency" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P "Scan through the device and check for internal consistency"
    ]
    @ help
  in
  ( Term.(ret (const Impl.check $ filename "test.qcow2"))
  , Cmd.info "check" ~sdocs:_common_options ~doc ~man
  )

let decode_cmd =
  let doc = "decode qcow2 formatted data and write a raw image" in
  let man =
    [
      `S "DESCRIPTION"; `P "Decode qcow2 formatted data and write to a raw file."
    ]
    @ help
  in
  ( Term.(ret (const Impl.decode $ filename "test.qcow2" $ output "test.raw"))
  , Cmd.info "decode" ~sdocs:_common_options ~doc ~man
  )

let stream_decode_cmd =
  let doc = "decode qcow2 formatted data from stdin and write a raw image" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P "Decode qcow2 formatted data from stdin and write to a raw file."
    ]
    @ help
  in
  let output default =
    let doc = Printf.sprintf "Path to the output file." in
    Arg.(value & pos 0 string default & info [] ~doc)
  in
  ( Term.(ret (const Impl.stream_decode $ output "test.raw"))
  , Cmd.info "stream_decode" ~sdocs:_common_options ~doc ~man
  )

let encode_cmd =
  let doc = "Convert the file from raw to qcow2" in
  let man = [`S "DESCRIPTION"; `P "Convert a raw file to qcow2 ."] @ help in
  ( Term.(ret (const Impl.encode $ filename "test.raw" $ output "test.qcow2"))
  , Cmd.info "encode" ~sdocs:_common_options ~doc ~man
  )

let create_cmd =
  let doc = "create a qcow-formatted data file" in
  let man = [`S "DESCRIPTION"; `P "Create a qcow-formatted data file"] @ help in
  ( Term.(
      ret
        (const Impl.create
        $ size
        $ strict_refcounts
        $ trace
        $ output "test.qcow2"
        )
    )
  , Cmd.info "create" ~sdocs:_common_options ~doc ~man
  )

let resize_cmd =
  let doc = "Change the maximum virtual size of the disk." in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "When a .qcow2 file is created, the physical file on disk is small but \
         the disk has a (usually much larger) 'virtual' size as seen from the \
         perspective of the client. A disk can usually be safely increased in \
         size without harming the contents. It's up to the client whether it \
         is able to use the new space or not."
    ]
    @ help
  in
  ( Term.(
      ret
        (const Impl.resize
        $ trace
        $ filename "test.qcow2"
        $ size
        $ ignore_data_loss
        )
    )
  , Cmd.info "resize" ~sdocs:_common_options ~doc ~man
  )

let unsafe_buffering =
  let doc =
    Printf.sprintf
      "Run faster by caching writes in memory. A failure in the middle could \
       corrupt the file."
  in
  Arg.(value & flag & info ["unsafe-buffering"] ~doc)

let discard_cmd =
  let doc = "Scan for zeroes and discard them" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "Iterate over all allocated blocks in the image, and if a block only \
         contains zeroes, then invoke discard (aka TRIM or UNMAP) on it. This \
         helps shrink the blocks in the file."
    ]
    @ help
  in
  ( Term.(ret (const Impl.discard $ unsafe_buffering $ filename "test.qcow2"))
  , Cmd.info "discard" ~sdocs:_common_options ~doc ~man
  )

let compact_cmd =
  let doc = "Compact the file" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "Iterate over all the unallocated blocks ('holes') in the file created \
         by discard and move live data into them to shrink the file."
    ]
    @ help
  in
  ( Term.(
      ret
        (const Impl.compact
        $ common_options_t
        $ unsafe_buffering
        $ filename "test.qcow2"
        )
    )
  , Cmd.info "compact" ~sdocs:_common_options ~doc ~man
  )

let repair_cmd =
  let doc = "Regenerate the refcount table in an image" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "Regenerate the refcount table in an image to make it compliant with \
         the spec. We normally avoid updating the refcount at runtime as a \
         performance optimisation."
    ]
    @ help
  in
  ( Term.(ret (const Impl.repair $ unsafe_buffering $ filename "test.qcow2"))
  , Cmd.info "repair" ~sdocs:_common_options ~doc ~man
  )

let sector =
  let doc = Printf.sprintf "Virtual sector within the qcow2 image" in
  Arg.(value & opt int64 0L & info ["sector"] ~doc)

let text =
  let doc = Printf.sprintf "Test to write into the qcow2 image" in
  Arg.(value & opt string "" & info ["text"] ~doc)

let write_cmd =
  let doc = "Write a string to a virtual address in a qcow2 image" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P "Write a string at a given virtual sector offset in the qcow2 image."
    ]
    @ help
  in
  ( Term.(ret (const Impl.write $ filename "test.qcow2" $ sector $ text $ trace))
  , Cmd.info "write" ~sdocs:_common_options ~doc ~man
  )

let length =
  let doc = Printf.sprintf "Length of the data in 512-byte sectors" in
  Arg.(value & opt int64 1L & info ["length"] ~doc)

let read_cmd =
  let doc = "Read a string from a virtual address in a qcow2 image" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P "Read a string at a given virtual sector offset in the qcow2 image."
    ]
    @ help
  in
  ( Term.(
      ret (const Impl.read $ filename "test.qcow2" $ sector $ length $ trace)
    )
  , Cmd.info "read" ~sdocs:_common_options ~doc ~man
  )

let mapped_cmd =
  let doc = "Output a list of allocated extents, which may contain writes" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "When a .qcow2 file is created, it is guaranteed to be full of zeroes. \
         As data is written to the virtual disk, metadata is updated on the \
         physical file which allows us to list the regions which have been \
         written to."
    ]
    @ help
  in
  ( Term.(
      ret
        (const Impl.mapped
        $ filename "test.qcow2"
        $ output_format
        $ ignore_zeroes
        )
    )
  , Cmd.info "mapped" ~sdocs:_common_options ~doc ~man
  )

let pattern_number =
  let doc = Printf.sprintf "Pattern number to write" in
  Arg.(value & opt int 1 & info ["pattern"] ~doc)

let pattern_cmd =
  let doc = "Generate a .qcow2 with a test pattern" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P "Create a qcow2 file with a test pattern."
    ; `P
        "Pattern 1: write to every other cluster to stress the metadata \
         datastructure."
    ; `P
        "Pattern 2: write to the whole disk and then discard every other \
         cluster to produce the worst case for compaction."
    ]
    @ help
  in
  ( Term.(
      ret
        (const Impl.pattern
        $ common_options_t
        $ trace
        $ output "test.qcow2"
        $ size
        $ pattern_number
        )
    )
  , Cmd.info "pattern" ~sdocs:_common_options ~doc ~man
  )

let sha_cmd =
  let doc = "Compute a SHA1 from the contents of a qcow2" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "This is equivalent to decoding the qcow2 to a raw file and running \
         sha1sum."
    ]
    @ help
  in
  ( Term.(ret (const Impl.sha $ common_options_t $ filename "test.qcow2"))
  , Cmd.info "sha" ~sdocs:_common_options ~doc ~man
  )

let dehydrate_cmd =
  let doc = "Extract only the metadata blocks for debugging" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "Create 2 files: one containing metadata blocks and the second \
         containing a map of block to physical offset in the file. When \
         rehydrated the resulting file has the same structure as the original, \
         but with none of the data. It is therefore safe to share the \
         dehydrated file with other people without fearing data leaks. "
    ; `P "To dehydrate a file input.qcow2 and produce dehydrated.{map,meta}:"
    ; `P "qcow-tool dehydrate input.qcow2 dehydrated"
    ]
    @ help
  in
  let output =
    let doc = Printf.sprintf "Prefix of the output files" in
    Arg.(value & pos 1 string "dehydrated" & info [] ~doc)
  in
  ( Term.(
      ret
        (const Impl.dehydrate
        $ common_options_t
        $ filename "test.qcow2"
        $ output
        )
    )
  , Cmd.info "dehydrate" ~sdocs:_common_options ~doc ~man
  )

let rehydrate_cmd =
  let doc = "Create a qcow2 file from a previously dehydrated file" in
  let man =
    [
      `S "DESCRIPTION"
    ; `P
        "Convert the files created by a previous call to dehydrate into a \
         valid qcow file which has the same structure as the original, but \
         with none of the data."
    ; `P "To rehydrate files dehydrated.{map,meta} into output.qcow2:"
    ; `P "qcow-tool rehydrate dehydrated output.qcow2"
    ]
    @ help
  in
  let filename =
    let doc = Printf.sprintf "Prefix of the input files" in
    Arg.(value & pos 0 string "dehydrated" & info [] ~doc)
  in
  let output =
    let doc = Printf.sprintf "Output qcow2 file" in
    Arg.(value & pos 1 string "output.qcow2" & info [] ~doc)
  in
  ( Term.(ret (const Impl.rehydrate $ common_options_t $ filename $ output))
  , Cmd.info "rehydrate" ~sdocs:_common_options ~doc ~man
  )

let cmds =
  [
    info_cmd
  ; create_cmd
  ; check_cmd
  ; repair_cmd
  ; encode_cmd
  ; stream_decode_cmd
  ; decode_cmd
  ; write_cmd
  ; read_cmd
  ; mapped_cmd
  ; resize_cmd
  ; discard_cmd
  ; compact_cmd
  ; pattern_cmd
  ; sha_cmd
  ; dehydrate_cmd
  ; rehydrate_cmd
  ]
  |> List.map (fun (t, i) -> Cmd.v i t)

let () =
  let default =
    Term.(ret (const (fun _ -> `Help (`Pager, None)) $ common_options_t))
  in
  let doc = "manipulate virtual disks stored in qcow2 files" in
  let man = help in
  let info =
    Cmd.info "qcow-tool" ~version:"1.0.0" ~sdocs:_common_options ~doc ~man
  in
  let cmd = Cmd.group ~default info cmds in
  exit (Cmd.eval cmd)
