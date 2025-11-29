open Base

type id_t = { millis : int; sequence : int } [@@deriving compare, equal, sexp]

type entry_t = { id : id_t; data : (string * string) list }
[@@deriving compare, equal, sexp]

type xrange_id_t = FromId | ToId

let id_to_string id = Stdlib.Printf.sprintf "%d-%d" id.millis id.sequence

let parse_entry_id (id : string) (last_entry_id : id_t) :
    (id_t, string) Result.t =
  let new_sequence millis =
    if last_entry_id.millis = millis then last_entry_id.sequence + 1 else 0
  in
  match String.split ~on:'-' id with
  | [ millis; "*" ] ->
      Ok
        {
          millis = Int.of_string millis;
          sequence = new_sequence (Int.of_string millis);
        }
  | [ millis; sequence ] ->
      Ok { millis = Int.of_string millis; sequence = Int.of_string sequence }
  | [ "*" ] ->
      let millis = Lifetime.now () in
      Ok { millis; sequence = new_sequence millis }
  | _ -> Error "Not a valid stream id"

let parse_xrange_id (id : string) (id_type : xrange_id_t) : id_t =
  match String.split ~on:'-' id with
  | [ millis; sequence ] ->
      { millis = Int.of_string millis; sequence = Int.of_string sequence }
  | [ millis ] -> (
      match id_type with
      | FromId -> { millis = Int.of_string millis; sequence = 0 }
      | ToId -> { millis = Int.of_string millis; sequence = Int.max_value })
  | _ -> { millis = 0; sequence = 0 }

let%test_unit "parse xrange full" =
  [%test_eq: id_t] (parse_xrange_id "15" FromId) { millis = 15; sequence = 0 }

let validate_entry_id (id : id_t) (reference_id : id_t) :
    (id_t, string) Result.t =
  if id.millis = 0 && id.sequence = 0 then
    Error "ERR The ID specified in XADD must be greater than 0-0"
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

let xrange (from_id : string) (to_id : string) (stream : entry_t list) :
    entry_t list =
  let from_id = parse_xrange_id from_id FromId in
  let to_id = parse_xrange_id to_id ToId in
  stream
  |> List.filter ~f:(fun entry ->
      compare_id_t entry.id from_id >= 0 && compare_id_t entry.id to_id <= 0)

let get_entry_id (entry : entry_t) : id_t = entry.id
let get_entry_data (entry : entry_t) : (string * string) list = entry.data
