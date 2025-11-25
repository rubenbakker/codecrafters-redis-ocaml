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

let normalize_lrange len from_idx to_idx =
  let from_idx =
    if from_idx >= 0 then from_idx else Int.max (len + from_idx) 0
  in
  let to_idx = if to_idx >= 0 then to_idx else Int.max (len + to_idx) 0 in
  if from_idx >= 0 && from_idx < len then
    let to_idx = Int.min to_idx (len - 1) in
    let len = to_idx - from_idx + 1 in
    Some (from_idx, len)
  else None

let lrange key from_idx to_idx =
  let from_idx = Int.of_string from_idx in
  let to_idx = Int.of_string to_idx in
  match Store.get key with
  | Some (Store.List l) -> (
      match normalize_lrange (List.length l) from_idx to_idx with
      | Some (pos, len) ->
          Resp.RespList
            (l |> List.sub ~pos ~len
            |> List.map ~f:(fun str -> Resp.BulkString str))
      | None -> RespList [])
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

let%test_unit "lrange positive idx" =
  [%test_eq: (int * int) option] (normalize_lrange 5 1 3) (Some (1, 3))

let%test_unit "lrange positive from_idx larger than list" =
  [%test_eq: (int * int) option] (normalize_lrange 5 1 7) (Some (1, 4))

let%test_unit "lrange positive from_idx larger than link" =
  [%test_eq: (int * int) option] (normalize_lrange 5 8 9) None

let%test_unit "lrange negative from_idx and to_idx" =
  [%test_eq: (int * int) option] (normalize_lrange 5 (-2) (-1)) (Some (3, 2))

let%test_unit "lrange negative from_idx and to_idx" =
  [%test_eq: (int * int) option] (normalize_lrange 5 (-7) 99) (Some (0, 5))
