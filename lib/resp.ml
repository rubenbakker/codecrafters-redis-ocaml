open Base

module Resp = struct
  type t = List of t list | String of string [@@deriving compare, equal, sexp]
end

exception InvalidData

type t = Resp.t

let string_regexp = Str.regexp "^\\$\\([0-9]+\\)\r\n\\(.*\\)\r\n"
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
    let _ = Str.string_match string_regexp str pos in
    ( String.length (Str.matched_group 0 str),
      Resp.String (Str.matched_group 2 str) )

let from_string str pos =
  let _, item = from_string_internal str pos in
  item

let rec to_string item =
  match item with
  | Resp.String str -> Printf.sprintf "$%d\r\n%s\r\n" (String.length str) str
  | Resp.List list ->
      Printf.sprintf "*%d\r\n%s" (List.length list)
        (String.concat ~sep:"" (List.map list ~f:(fun x -> to_string x)))

let command str =
  match from_string str 0 with
  | Resp.List [ Resp.String command ] -> (String.lowercase command, "")
  | Resp.List [ Resp.String command; Resp.String arg ] ->
      (String.lowercase command, arg)
  | Resp.String command -> (String.lowercase command, "")
  | _ -> raise InvalidData

let string_to_resp str = Resp.String str |> to_string

let%test_unit "from_string string" =
  [%test_eq: Resp.t] (from_string "$3\r\nHey\r\n" 0) (Resp.String "Hey")

let%test_unit "to_string string" =
  [%test_eq: string] (to_string (Resp.String "Hey")) "$3\r\nHey\r\n"

let%test_unit "from_string list" =
  [%test_eq: Resp.t]
    (from_string "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n" 0)
    (Resp.List [ Resp.String "hello"; Resp.String "world" ])

let%test_unit "from_string list" =
  [%test_eq: string]
    (to_string (Resp.List [ Resp.String "hello"; Resp.String "world" ]))
    "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
