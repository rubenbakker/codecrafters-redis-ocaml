open! Base

let%expect_test "read a integer" =
  let channel = Stdlib.Scanf.Scanning.from_string ":55\r\n" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (Integer 55) |}]

let%expect_test "read a simple string" =
  let channel = Stdlib.Scanf.Scanning.from_string "+OK\r\n" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (SimpleString OK) |}]

let%expect_test "read a bulk string" =
  let channel = Stdlib.Scanf.Scanning.from_string "$3\r\nHey\r\n" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (BulkString Hey) |}]

let%expect_test "read an array of bulk strings" =
  let channel =
    Stdlib.Scanf.Scanning.from_string
      "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
  in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect
    {| (RespList((BulkString REPLCONF)(BulkString ACK)(BulkString 0))) |}]

let%expect_test "read a binary" =
  let channel = Stdlib.Scanf.Scanning.from_string "$4\r\n1234" in
  let result = Resp.read_from_channel channel in
  Stdlib.print_endline (result |> Resp.to_sexp |> Sexp.to_string);
  [%expect {| (RespBinary 1234) |}]
