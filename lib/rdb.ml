open Base

let empty_rdb =
  Base64.decode_exn
    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

(* let read (rdb : string) = *)
(*   let bs = Bitstring.bitstring_of_string rdb in *)
(*   match%bitstring bs with *)
(*   | {| "REDIS" : 5*8 : string ; version : 4*8 : string |} -> *)
(*       Stdlib.Printf.printf "Version: %s\n" version *)

let ( let* ) x f = Stdlib.Option.bind x f

let skip_to (inch : In_channel.t) (char : Char.t) : Char.t option =
  let rec loop _ =
    match In_channel.input_char inch with
    | Some ch -> if Char.(ch = char) then Some ch else loop ()
    | None -> None
  in
  loop ()

let echo = Stdlib.Printf.printf

let read_length (inch : In_channel.t) : int option =
  let* ch = In_channel.input_char inch in
  let* int_value = Some (Char.to_int ch) in
  match int_value with
  | v when 0x00000000 lor (v land 0x11000000) = 0 -> Some (0b00111111 land v)
  | v when 0x01000000 lor v > 0 -> (
      match In_channel.input_char inch with
      | Some next_char ->
          0b00111111 land v |> fun i ->
          i lsl 8 |> fun i ->
          i lor Char.to_int next_char |> fun result -> Some result
      | None -> None)
  | _ -> None

exception Debug

let read_string (inch : In_channel.t) : string option =
  let* length = read_length inch in
  In_channel.really_input_string inch length

type t = {
  magic : string;
  version : string;
  db_index : int;
  hash_table : (string * string) list;
}
[@@deriving sexp]

let read (rdb_path : string) : t option =
  In_channel.with_open_bin rdb_path (fun inch ->
      let* magic = In_channel.really_input_string inch 5 in
      let* version = In_channel.really_input_string inch 4 in
      let* db_section = skip_to inch (Char.of_int_exn 0xFE) in
      if Char.to_int db_section <> 0xFE then raise Debug;
      let* db_index = read_length inch in
      (* if db_index <> 0 then raise Debug; *)
      let* hash_table_section = skip_to inch (Char.of_int_exn 0xFB) in
      if Char.to_int hash_table_section <> 0xFB then raise Debug;
      let* hash_table_length = read_length inch in
      let* _expiry_table_length = read_length inch in
      let hash_table =
        List.range 0 hash_table_length
        |> List.map ~f:(fun _ ->
               let _type_char = In_channel.input_char inch in
               ( Option.value_exn (read_string inch),
                 Option.value_exn (read_string inch) ))
      in
      Some { magic; version; db_index; hash_table })
