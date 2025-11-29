open Base

type id_t = { millis : int; sequence : int }
type entry_t = { id : id_t; data : (string * string) list }

let id_to_string id = Stdlib.Printf.sprintf "%d-%d" id.millis id.sequence

let parse_entry_id (id : string) (last_entry_id : id_t) :
    (id_t, string) Result.t =
  match String.split ~on:'-' id with
  | [ millis; "*" ] ->
      let new_sequence =
        if last_entry_id.millis = Int.of_string millis then
          last_entry_id.sequence + 1
        else 0
      in
      Ok { millis = Int.of_string millis; sequence = new_sequence }
  | [ millis; sequence ] ->
      Ok { millis = Int.of_string millis; sequence = Int.of_string sequence }
  | _ -> Error "Not a valid stream id"

let validate_entry_id (id : id_t) (reference_id : id_t) :
    (id_t, string) Result.t =
  if
    id.millis = 0 && id.sequence = 0 && reference_id.millis = 0
    && reference_id.sequence = 0
  then Error "ERR The ID specified in XADD must be greater than 0-0"
  else if id.millis > reference_id.millis then Ok id
  else if id.millis = reference_id.millis then
    if id.sequence > reference_id.sequence then Ok id
    else
      Error
        "ERR The ID specified in XADD is equal or smaller than the target \
         stream top item"
  else
    Error
      "ERR The ID specified in XADD is equal or smaller than the target stream \
       top item"

let add_entry_to_stream (id : string) (data : (string * string) list)
    (stream : entry_t list) : (string * entry_t list, string) Result.t =
  let last_entry_id =
    match List.last stream with
    | Some entry -> entry.id
    | None -> { millis = 0; sequence = 0 }
  in
  match parse_entry_id id last_entry_id with
  | Ok id -> (
      match validate_entry_id id last_entry_id with
      | Ok id ->
          let new_stream = stream @ [ { id; data } ] in
          let id = id_to_string id in
          Ok (id, new_stream)
      | Error error -> Error error)
  | Error error -> Error error
