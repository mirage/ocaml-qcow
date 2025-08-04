FROM ocaml/opam:alpine-ocaml-4.12 AS build
# By default the container uses opam 2.0, change that
RUN sudo ln -f /usr/bin/opam-2.3 /usr/bin/opam && opam init --reinit -ni
WORKDIR /src
COPY . .

RUN opam pin add . -n --with-version=~dev
RUN opam install .

FROM alpine:latest
COPY --from=build /home/opam/.opam/4.12/bin/qcow-tool /
ENTRYPOINT ["/qcow-tool"]
