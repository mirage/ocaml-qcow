(lang dune 3.18)

(cram enable)
(subst disabled)

(name qcow)
(formatting (enabled_for ocaml))
(generate_opam_files true)

(package
  (name qcow-types)
  (synopsis "Minimal set of dependencies for qcow-stream, shared with qcow")
  (maintainers "Dave Scott <dave@recoil.org>" "Pau Ruiz Safont" "Edwin Török ")
  (tags ("org:mirage"))
  (homepage "https://github.com/mirage/ocaml-qcow")
  (source (github mirage/ocaml-qcow))
  (license "ISC")
  (authors "David Scott")
  (maintenance_intent "latest")
  (depends
    (ocaml (>= 4.12.0))
    astring
    (cstruct (>= 6.1.0))
    (diet (>= 0.3.0))
    logs
    lwt
    (mirage-block (>= 3.0.0))
    ppx_sexp_conv
    prometheus
    sexplib
  )
)

(package
  (name qcow-stream)
  (synopsis "Library offering QCOW streaming capabilities")
  (maintainers "Dave Scott <dave@recoil.org>" "Pau Ruiz Safont" "Edwin Török ")
  (tags ("org:mirage"))
  (homepage "https://github.com/mirage/ocaml-qcow")
  (source (github mirage/ocaml-qcow))
  (license "ISC")
  (authors "David Scott")
  (maintenance_intent "latest")
  (depends
    (qcow-types (= :version))
    cstruct-lwt
    io-page
    (lwt (>= 5.5.0))
  )
)

(package
  (name qcow-tool)
  (synopsis "A command-line tool for manipulating qcow2-formatted data")
  (maintainers "Dave Scott <dave@recoil.org>" "Pau Ruiz Safont" "Edwin Török ")
  (tags ("org:mirage"))
  (homepage "https://github.com/mirage/ocaml-qcow")
  (source (github mirage/ocaml-qcow))
  (license "ISC")
  (authors "David Scott")
  (maintenance_intent "latest")
  (depends
    (ocaml (>= 4.12.0))
    (qcow (= :version))
    (qcow-stream (= :version))
    (conf-qemu-img :with-test)
    (cmdliner (and (>= 1.1.0) (< 2.0.0)))
    cstruct
    result
    unix-type-representations
    lwt
    (mirage-block (>= 3.0.0))
    (sha (>= 1.10))
    sexplib
    logs
    (fmt (>= 0.8.2))
    astring
    (io-page (>= 2.4.0))
    (ounit :with-test)
    (mirage-block-ramdisk :with-test)
    (ezjsonm :with-test)
  )
)

(package
  (name qcow)
  (synopsis "Support for Qcow2 images")
  (maintainers "Dave Scott <dave@recoil.org>" "Pau Ruiz Safont" "Edwin Török ")
  (tags ("org:mirage"))
  (homepage "https://github.com/mirage/ocaml-qcow")
  (source (github mirage/ocaml-qcow))
  (license "ISC")
  (authors "David Scott")
  (maintenance_intent "latest")
  (depends
    (ocaml (>= 4.12.0))
    (qcow-types (= :version))
    base-bytes
    (cstruct (>= 3.4.0))
    result
    (io-page (>= 2.4.0))
    (lwt (>= 5.5.0))
    (mirage-block (>= 3.0.0))
    (mirage-block-unix (>= 2.5.0))
    mirage-block-combinators
    mirage-sleep
    sexplib
    logs
    (fmt (>= 0.8.2))
    astring
    prometheus
    unix-type-representations
    stdlib-shims
    sha
    ppx_deriving
    ppx_sexp_conv
    (ounit :with-test)
    (mirage-block-ramdisk (and :with-test (>= 0.5)))
    (ezjsonm :with-test)
  )
)
