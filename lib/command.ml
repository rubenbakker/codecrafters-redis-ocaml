open Base

let process_ping () = Resp.SimpleString "PONG"

let set ~expiry key value =
  let expiry =
    match expiry with
    | Some (expiry_type, expiry_value) ->
        Lifetime.create_expiry expiry_type expiry_value
    | None -> Lifetime.Forever
  in
  Store.set key (Store.String value) expiry;
  Resp.SimpleString "OK"

let get key =
  let value = Store.get key in
  match value with None -> Resp.Null | Some v -> Resp.from_store v

let rpush key rest =
  let exiting_list = Store.get key in
  let new_list =
    match exiting_list with Some (Store.List l) -> l @ rest | _ -> rest
  in
  Store.set key (Store.List new_list) Lifetime.Forever;
  Resp.Integer (List.length new_list)

let lrange key from_idx to_idx =
  let pos = Int.of_string from_idx in
  let to_idx = Int.of_string to_idx in
  match Store.get key with
  | Some (Store.List l) when List.length l > pos ->
      let to_idx = Int.min to_idx (List.length l - 1) in
      let len = to_idx - pos + 1 in
      Resp.RespList
        (l |> List.sub ~pos ~len |> List.map ~f:(fun str -> Resp.BulkString str))
  | _ -> Resp.RespList []

let echo message = Resp.BulkString message

let process str =
  let command = Resp.command str in
  match command with
  | "ping", [] -> process_ping ()
  | "set", [ key; value ] -> set key value ~expiry:None
  | "set", [ key; value; expiry_type; expiry_value ] ->
      set ~expiry:(Some (expiry_type, expiry_value)) key value
  | "get", [ key ] -> get key
  | "rpush", key :: rest -> rpush key rest
  | "lrange", [ key; from_idx; to_idx ] -> lrange key from_idx to_idx
  | "echo", [ message ] -> echo message
  | _ -> Resp.Null
