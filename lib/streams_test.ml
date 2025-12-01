open! Base

let create_sample_stream () =
  Some
    (Streams.create
       [
         ("0-1", [ ("hello", "world") ]);
         ("0-2", [ ("bar", "baz") ]);
         ("1-1", [ ("xx", "fff") ]);
       ])

let%expect_test "xrange all data" =
  let resp = Streams.xrange "-" "+" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp resp |> Sexp.to_string_hum);
  [%expect
    {|
    (RespList
     ((RespList
       ((BulkString 0-1) (RespList ((BulkString hello) (BulkString world)))))
      (RespList
       ((BulkString 0-2) (RespList ((BulkString bar) (BulkString baz)))))
      (RespList ((BulkString 1-1) (RespList ((BulkString xx) (BulkString fff)))))))
    |}]

let%expect_test "xrange from id" =
  let resp = Streams.xrange "0-2" "+" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp resp |> Sexp.to_string_hum);
  [%expect
    {|
    (RespList
     ((RespList
       ((BulkString 0-2) (RespList ((BulkString bar) (BulkString baz)))))
      (RespList ((BulkString 1-1) (RespList ((BulkString xx) (BulkString fff)))))))
    |}]

let%expect_test "xrange to id" =
  let resp = Streams.xrange "-" "0-2" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp resp |> Sexp.to_string_hum);
  [%expect
    {|
    (RespList
     ((RespList
       ((BulkString 0-1) (RespList ((BulkString hello) (BulkString world)))))
      (RespList
       ((BulkString 0-2) (RespList ((BulkString bar) (BulkString baz)))))))
    |}]
