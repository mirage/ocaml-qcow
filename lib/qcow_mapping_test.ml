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

let interval_test () =
  (* Set a few of the cells, representing a sparse disk *)
  let arr = Arr.create 16L in
  for i = 2 to 4 do
    let i = Int64.of_int i in
    Arr.set arr i i
  done ;

  (* Non-subsequent clusters next to each other *)
  Arr.set arr 6L 0L ;
  Arr.set arr 7L 6L ;
  Arr.set arr 8L 8L ;

  for i = 10 to 13 do
    let i = Int64.of_int i in
    Arr.set arr i i
  done ;

  (* Verify only the intervals filled with subsequent clusters
     are reported as populated *)
  let l = Arr.to_interval_seq arr 0L |> List.of_seq in
  Alcotest.(check' @@ list @@ pair int64 int64)
    ~msg:"wrong interval" ~actual:l
    ~expected:[(2L, 4L); (0L, 0L); (6L, 6L); (8L, 8L); (10L, 13L)] ;
  ()

let () =
  Alcotest.run "Test qcow_mapping library"
    [
      ( "Basic tests"
      , [
          ("basic_test", `Quick, basic_test)
        ; ("interval_test", `Quick, interval_test)
        ]
      )
    ]
