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
  let result = Streams.xrange "-" "+" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp result.return |> Sexp.to_string_hum);
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
  let result = Streams.xrange "0-2" "+" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp result.return |> Sexp.to_string_hum);
  [%expect
    {|
    (RespList
     ((RespList
       ((BulkString 0-2) (RespList ((BulkString bar) (BulkString baz)))))
      (RespList ((BulkString 1-1) (RespList ((BulkString xx) (BulkString fff)))))))
    |}]

let%expect_test "xrange to id" =
  let result = Streams.xrange "-" "0-2" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp result.return |> Sexp.to_string_hum);
  [%expect
    {|
    (RespList
     ((RespList
       ((BulkString 0-1) (RespList ((BulkString hello) (BulkString world)))))
      (RespList
       ((BulkString 0-2) (RespList ((BulkString bar) (BulkString baz)))))))
    |}]

let%expect_test "xadd on empty list with listeners" =
  let result =
    Streams.xadd "0-1" [ "hello"; "world" ] 1 (Some (Streams.empty ()))
  in
  Stdio.print_endline
    (Streams.to_sexp (Option.value_exn result.store) |> Sexp.to_string_hum);
  [%expect {| (((id ((millis 0) (sequence 1))) (data ((hello world))))) |}];
  Stdio.print_endline (Resp.to_sexp result.return |> Sexp.to_string_hum);
  [%expect {| (BulkString 0-1) |}];
  List.iter result.notify_with ~f:(fun resp ->
      Stdio.print_endline (Resp.to_sexp resp |> Sexp.to_string_hum));
  [%expect
    {|
    (RespList
     ((RespList
       ((BulkString 0-1) (RespList ((BulkString hello) (BulkString world)))))))
    |}]

let%expect_test "xread existing" =
  let result = Streams.xread "key" "0-1" (create_sample_stream ()) in
  Stdio.print_endline (Resp.to_sexp result.return |> Sexp.to_string_hum);
  [%expect
    {|
    (RespList
     ((BulkString key)
      (RespList
       ((RespList
         ((BulkString 0-2) (RespList ((BulkString bar) (BulkString baz)))))
        (RespList
         ((BulkString 1-1) (RespList ((BulkString xx) (BulkString fff)))))))))
    |}]
