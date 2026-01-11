open Base

type t =
  | RespList of t list
  | BulkString of string
  | SimpleString of string
  | Integer of int
  | Null
  | NullArray
  | RespBinary of string
  | RespConcat of t list
  | RespError of string
  | RespIgnore
[@@deriving compare, equal, sexp]

exception InvalidData

let null_string = "$-1\r\n"

let rec to_string (item : t) : string =
  match item with
  | Integer integer_value ->
      Printf.sprintf ":%s%d\r\n"
        (if integer_value < 0 then "-" else "")
        integer_value
  | SimpleString str -> Printf.sprintf "+%s\r\n" str
  | BulkString str -> Printf.sprintf "$%d\r\n%s\r\n" (String.length str) str
  | RespList list ->
      Printf.sprintf "*%d\r\n%s" (List.length list)
        (String.concat ~sep:"" (List.map ~f:(fun x -> to_string x) list))
  | Null -> null_string
  | NullArray -> "*-1\r\n"
  | RespBinary str -> Printf.sprintf "$%d\r\n%s" (String.length str) str
  | RespConcat list ->
      String.concat ~sep:"" (List.map ~f:(fun x -> to_string x) list)
  | RespError error -> Printf.sprintf "-%s\r\n" error
  | RespIgnore -> ""

let to_sexp (input : t) : Sexp.t = sexp_of_t input
let to_simple_string (str : string) : string = SimpleString str |> to_string
let to_bulk_string (str : string) : string = BulkString str |> to_string
let to_integer_string (value : int) : string = Integer value |> to_string

let remove_trailing_cr value =
  String.sub ~pos:0 ~len:(String.length value - 1) value

let read_line_int (channel : Stdlib.in_channel) : int =
  let str = Stdlib.input_line channel in
  Int.of_string (remove_trailing_cr str)

let read_binary_from_channel (channel : Stdlib.in_channel) : t * int =
  let before = Stdlib.pos_in channel in
  match Stdlib.input_char channel with
  | '$' ->
      let count = read_line_int channel in
      let str = Stdlib.really_input_string channel count in
      let after = Stdlib.pos_in channel in
      (RespBinary str, after - before)
  | _ -> raise InvalidData

let rec read_from_channel (channel : Stdlib.in_channel) : t * int =
  let before = Stdlib.pos_in channel in
  let result =
    match Stdlib.input_char channel with
    | ':' -> read_line_int channel |> fun value -> Integer value
    | '+' ->
        Stdlib.input_line channel |> remove_trailing_cr |> fun value ->
        SimpleString value
    | '-' ->
        Stdlib.input_line channel |> remove_trailing_cr |> fun value ->
        RespError value
    | '$' ->
        let count = read_line_int channel in
        let str = Stdlib.really_input_string channel count in
        ignore @@ Stdlib.really_input_string channel 2;
        (* remove \r\n from tail *)
        BulkString str
    | '*' ->
        let count = read_line_int channel in
        List.range 0 count
        |> List.map ~f:(fun _ ->
               let result, _ = read_from_channel channel in
               result)
        |> fun l -> RespList l
    | _ -> RespError "Error: not implemented"
  in
  let after = Stdlib.pos_in channel in
  (result, after - before)

let%test_unit "to_string bulk string" =
  [%test_eq: string] (to_string (BulkString "Hey")) "$3\r\nHey\r\n"

let%test_unit "to_string binary" =
  [%test_eq: string] (to_string (RespBinary "Hey")) "$3\r\nHey"

let%test_unit "to_string concat" =
  [%test_eq: string]
    (to_string (RespConcat [ BulkString "Hey"; RespBinary "xxx" ]))
    "$3\r\nHey\r\n$3\r\nxxx"

let%test_unit "to_string simple string" =
  [%test_eq: string] (to_string (SimpleString "Hey")) "+Hey\r\n"

let%test_unit "to_string integer" =
  [%test_eq: string] (to_string (Integer 55)) ":55\r\n"

let%test_unit "to_string error" =
  [%test_eq: string] (to_string (RespError "Error 77")) "-Error 77\r\n"

let%test_unit "to_string list with 2 bulk strings" =
  [%test_eq: string]
    (to_string (RespList [ BulkString "hello"; BulkString "world" ]))
    "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
