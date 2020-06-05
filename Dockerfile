FROM ocaml/opam2:alpine

RUN opam install depext
COPY . /src
RUN opam pin add qcow /src -n
RUN opam depext -i qcow
RUN opam pin add qcow-tool /src -n
RUN opam depext -i qcow-tool
