module Arr = Qcow_mapping

let ( ++ ) = Int64.add

let basic_test () =
  let arr = Arr.create 16L in

  Alcotest.(check' int64)
    ~msg:"wrong length" ~actual:(Arr.length arr) ~expected:16L ;

  for i = 0 to Int64.to_int (Arr.length arr) - 1 do
    let i = Int64.of_int i in
    Arr.set arr i i
  done ;
  for i = 0 to Int64.to_int (Arr.length arr) - 1 do
    let i = Int64.of_int i in
    let v = Arr.get arr i in
    Alcotest.(check' int64) ~msg:"wrong value" ~actual:v ~expected:i
  done ;

  Arr.extend arr 32L ;
  Alcotest.(check' int64)
    ~msg:"wrong length after .extend" ~actual:(Arr.length arr) ~expected:32L ;

  (* Check values were copied properly *)
  for i = 0 to 15 do
    let i = Int64.of_int i in
    let v = Arr.get arr i in
    Alcotest.(check' int64)
      ~msg:"wrong value after .extend" ~actual:v ~expected:i
  done ;

  for i = 0 to Int64.to_int (Arr.length arr) - 1 do
    let i = Int64.of_int i in
    Arr.set arr i (i ++ 1L)
  done ;
  for i = 0 to Int64.to_int (Arr.length arr) - 1 do
    let i = Int64.of_int i in
    let v = Arr.get arr i in
    Alcotest.(check' int64) ~msg:"wrong value" ~actual:v ~expected:(i ++ 1L)
  done ;

  ()

let () =
  Alcotest.run "Test qcow_mapping library"
    [("Basic tests", [("test", `Quick, basic_test)])]
