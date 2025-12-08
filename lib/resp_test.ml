open! Base

let%expect_test "read a integer" =
  let channel = Stdlib.Scanf.Scanning.from_string ":55\r\n" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (Integer 55) |}]

let%expect_test "read a integer" =
  let channel = Stdlib.Scanf.Scanning.from_string "$3\r\nHey\r\n" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (BulkString Hey) |}]

let%expect_test "read an array of bulk strings" =
  let channel = Stdlib.Scanf.Scanning.from_string "*3\r\nHey\r\n" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (BulkString Hey) |}]

let%expect_test "read a bulk string" =
  let channel = Stdlib.Scanf.Scanning.from_string "*3\r\nHey\r\n" in
  Stdlib.Stdlib.print_endline unit;
  [%expect
    {| unit | }]

let%expect_test "read a bulk string" =
  let number, value =
    Stdlib.Scanf.sscanf "$3\r\nHey\r\n" "$%d\r\n%s\r\n" (fun d s -> (d, s))
  in
  Stdlib.print_string "size:";
  Stdlib.print_int number;
  Stdlib.print_string "value:";
  Stdlib.print_string value;
  [%expect {| size:3value:Hey |}]
