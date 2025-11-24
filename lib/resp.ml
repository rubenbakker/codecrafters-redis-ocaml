open Base

module Resp = struct
  type t = List of t list | BulkString of string | SimpleString of string
  [@@deriving compare, equal, sexp]
end

let null_string = "$-1\r\n"

exception InvalidData

type t = Resp.t

let simple_string_regex = Str.regexp "^\\+\\(.*\\)\r\n"
let bulk_string_regex = Str.regexp "^\\$\\([0-9]+\\)\r\n\\(.*\\)\r\n"
let list_regexp = Str.regexp "^\\*\\([0-9]+\\)\r\n"

let rec from_string_internal str pos =
  let list_result = Str.string_match list_regexp str pos in
  if list_result then (
    let prefix_length = String.length (Str.matched_group 0 str) in
    let count = Stdlib.int_of_string (Str.matched_group 1 str) in
    let list = ref [] in
    let total_length = ref 0 in
    for _idx = 0 to count - 1 do
      let length, item =
        from_string_internal str (pos + prefix_length + !total_length)
      in
      total_length := !total_length + length;
      list := item :: !list
    done;
    (!total_length + prefix_length, Resp.List (List.rev !list)))
  else
    let simple_string_result = Str.string_match simple_string_regex str pos in
    if simple_string_result then
      ( String.length (Str.matched_group 0 str),
        Resp.SimpleString (Str.matched_group 1 str) )
    else
      let string_result = Str.string_match bulk_string_regex str pos in
      if string_result then
        ( String.length (Str.matched_group 0 str),
          Resp.BulkString (Str.matched_group 2 str) )
      else (0, Resp.BulkString "")

let from_string str pos =
  let _, item = from_string_internal str pos in
  item

let rec to_string item =
  match item with
  | Resp.SimpleString str -> Printf.sprintf "+%s\r\n" str
  | Resp.BulkString str ->
      Printf.sprintf "$%d\r\n%s\r\n" (String.length str) str
  | Resp.List list ->
      Printf.sprintf "*%d\r\n%s" (List.length list)
        (String.concat ~sep:"" (List.map list ~f:(fun x -> to_string x)))

let arg arg =
  match arg with
  | Resp.BulkString str -> str
  | Resp.SimpleString str -> str
  | Resp.List _ -> ""

let command str =
  let parsed_command = from_string str 0 in
  match parsed_command with
  | Resp.List (Resp.BulkString command :: rest) ->
      (String.lowercase command, List.map ~f:arg rest)
  | _ -> raise InvalidData

let to_simple_string str = Resp.SimpleString str |> to_string
let to_bulk_string str = Resp.BulkString str |> to_string

let%test_unit "from_string simple string" =
  [%test_eq: Resp.t] (from_string "+Hey\r\n" 0) (Resp.SimpleString "Hey")

let%test_unit "from_string bulk string" =
  [%test_eq: Resp.t] (from_string "$3\r\nHey\r\n" 0) (Resp.BulkString "Hey")

let%test_unit "to_string bulk string" =
  [%test_eq: string] (to_string (Resp.BulkString "Hey")) "$3\r\nHey\r\n"

let%test_unit "to_string simple string" =
  [%test_eq: string] (to_string (Resp.SimpleString "Hey")) "+Hey\r\n"

let%test_unit "from_string list" =
  [%test_eq: Resp.t]
    (from_string "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n" 0)
    (Resp.List [ Resp.BulkString "hello"; Resp.BulkString "world" ])

let%test_unit "from_string list with 2 bulk strings" =
  [%test_eq: string]
    (to_string (Resp.List [ Resp.BulkString "hello"; Resp.BulkString "world" ]))
    "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
