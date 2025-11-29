open Base

let process_ping () : Resp.t = Resp.SimpleString "PONG"

let set ~(expiry : (string * string) option) (key : string) (value : string) :
    Resp.t =
  let expiry =
    match expiry with
    | Some (expiry_type, expiry_value) ->
        Lifetime.create_expiry expiry_type expiry_value
    | None -> Lifetime.Forever
  in
  Store.set key (Store.StorageString value) expiry;
  Resp.SimpleString "OK"

let get (key : string) : Resp.t =
  let value = Store.get key in
  match value with None -> Resp.Null | Some v -> Resp.from_store v

let rpush (key : string) (rest : string list) : Resp.t =
  let list_count = Store.rpush key rest in
  Resp.Integer list_count

let lpush (key : string) (rest : string list) =
  let list_count = Store.lpush key rest in
  Resp.Integer list_count

let normalize_lrange (len : int) (from_idx : int) (to_idx : int) :
    (int * int) option =
  let from_idx =
    if from_idx >= 0 then from_idx else Int.max (len + from_idx) 0
  in
  let to_idx = if to_idx >= 0 then to_idx else Int.max (len + to_idx) 0 in
  if from_idx >= 0 && from_idx < len then
    let to_idx = Int.min to_idx (len - 1) in
    let len = to_idx - from_idx + 1 in
    Some (from_idx, len)
  else None

let lrange (key : string) (from_idx : string) (to_idx : string) : Resp.t =
  let from_idx = Int.of_string from_idx in
  let to_idx = Int.of_string to_idx in
  match Store.get key with
  | Some (Store.StorageList l) -> (
      match normalize_lrange (List.length l) from_idx to_idx with
      | Some (pos, len) ->
          Resp.RespList
            (l |> List.sub ~pos ~len
            |> List.map ~f:(fun str -> Resp.BulkString str))
      | None -> RespList [])
  | _ -> Resp.RespList []

let llen (key : string) : Resp.t =
  match Store.get key with
  | Some (Store.StorageList l) -> Resp.Integer (List.length l)
  | _ -> Resp.Integer 0

let lpop (key : string) (count : int) : Resp.t =
  match Store.get key with
  | Some (Store.StorageList existing_list) -> (
      match existing_list with
      | first :: rest as l -> (
          match count with
          | 1 ->
              Store.set key (Store.StorageList rest) Lifetime.Forever;
              Resp.BulkString first
          | _ ->
              let count = Int.min (List.length l) count in
              let result = List.take l count in
              let new_list =
                List.sub ~pos:count ~len:(List.length l - count) l
              in
              Store.set key (Store.StorageList new_list) Lifetime.Forever;
              Resp.RespList
                (List.map ~f:(fun str -> Resp.BulkString str) result))
      | _ -> Resp.Null)
  | _ -> Resp.Null

let blpop (key : string) (timeout : string) : Resp.t =
  let timeout =
    match Float.of_string_opt timeout with
    | Some v -> v
    | None -> Int.of_string timeout |> Float.of_int
  in
  let item = Store.pop_or_wait key timeout in
  match item with
  | Some v -> Resp.RespList [ Resp.BulkString key; Resp.from_store v ]
  | None -> Resp.NullArray

let type_cmd (key : string) : Resp.t =
  (match Store.get key with
    | None -> "none"
    | Some v -> (
        match v with
        | Store.StorageList _ -> "list"
        | Store.StorageString _ -> "string"
        | Store.StorageStream _ -> "stream"))
  |> fun v -> Resp.SimpleString v

let echo (message : string) : Resp.t = Resp.BulkString message

let xadd (key : string) (id : string) (rest : string list) : Resp.t =
  match Store.xadd key id rest with
  | Ok (id, _) -> Resp.BulkString id
  | Error error -> Resp.RespError error

let process (str : string) : Resp.t =
  let command = Resp.command str in
  match command with
  | "ping", [] -> process_ping ()
  | "set", [ key; value ] -> set key value ~expiry:None
  | "set", [ key; value; expiry_type; expiry_value ] ->
      set ~expiry:(Some (expiry_type, expiry_value)) key value
  | "get", [ key ] -> get key
  | "rpush", key :: rest -> rpush key rest
  | "lpush", key :: rest -> lpush key rest
  | "lrange", [ key; from_idx; to_idx ] -> lrange key from_idx to_idx
  | "llen", [ key ] -> llen key
  | "lpop", [ key ] -> lpop key 1
  | "lpop", [ key; count ] -> lpop key (Int.of_string count)
  | "blpop", [ key; timeout ] -> blpop key timeout
  | "type", [ key ] -> type_cmd key
  | "echo", [ message ] -> echo message
  | "xadd", key :: id :: rest -> xadd key id rest
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
