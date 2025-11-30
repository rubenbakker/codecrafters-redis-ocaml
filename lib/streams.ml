open Base

type id_t = { millis : int; sequence : int } [@@deriving compare, equal, sexp]

type entry_t = { id : id_t; data : (string * string) list }
[@@deriving compare, equal, sexp]

type t = entry_t list
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
  match id with
  | "-" -> { millis = 0; sequence = 0 }
  | "+" -> { millis = Int.max_value; sequence = Int.max_value }
  | _ -> (
      match String.split ~on:'-' id with
      | [ millis; sequence ] ->
          { millis = Int.of_string millis; sequence = Int.of_string sequence }
      | [ millis ] -> (
          match id_type with
          | FromId -> { millis = Int.of_string millis; sequence = 0 }
          | ToId -> { millis = Int.of_string millis; sequence = Int.max_value })
      | _ -> { millis = 0; sequence = 0 })

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

let xadd (id : string) (data : string list) (_listener_count : int)
    (stream : t option) : t option * Resp.t * Resp.t list =
  let stream_data = match stream with Some stream -> stream | None -> [] in
  let last_entry_id =
    match List.last stream_data with
    | Some entry -> entry.id
    | None -> { millis = 0; sequence = 0 }
  in
  let data = Listutils.make_pairs data in
  match parse_entry_id id last_entry_id with
  | Ok id -> (
      match validate_entry_id id last_entry_id with
      | Ok id ->
          let new_stream = stream_data @ [ { id; data } ] in
          let id = id_to_string id in
          (Some new_stream, Resp.BulkString id, [])
      | Error error -> (Some stream_data, Resp.RespError error, []))
  | Error error -> (None, Resp.RespError error, [])

let entries_to_resp (entries : entry_t list) : Resp.t =
  entries
  |> List.map ~f:(fun entry ->
         let (data : Resp.t list) =
           List.fold entry.data ~init:[]
             ~f:(fun (acc : Resp.t list) data_pair ->
               let key, value = data_pair in
               List.append acc [ Resp.BulkString key; Resp.BulkString value ])
         in
         Resp.RespList
           [ Resp.BulkString (id_to_string entry.id); Resp.RespList data ])
  |> fun l -> Resp.RespList l

let xrange (from_id : string) (to_id : string) (stream : t option) : Resp.t =
  match stream with
  | Some stream ->
      let from_id = parse_xrange_id from_id FromId in
      let to_id = parse_xrange_id to_id ToId in
      stream
      |> List.filter ~f:(fun entry ->
             compare_id_t entry.id from_id >= 0
             && compare_id_t entry.id to_id <= 0)
      |> entries_to_resp
      |> fun resp -> resp
  | None -> Resp.RespError "key not found"

let xread (key : string) (from_id : string) (stream : t option) : Resp.t =
  match stream with
  | Some stream ->
      let from_id = parse_xrange_id from_id FromId in
      stream
      |> List.filter ~f:(fun entry -> compare_id_t entry.id from_id > 0)
      |> entries_to_resp
      |> fun resp -> Resp.RespList [ Resp.BulkString key; resp ]
  | None -> Resp.RespError "key not found"
