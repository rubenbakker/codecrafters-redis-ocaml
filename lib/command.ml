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

let resp_from_store (item : Store.t) : Resp.t =
  match item with
  | Store.StorageString str -> Resp.BulkString str
  | Store.StorageList l -> Lists.to_resp l
  | Store.StorageStream _ -> Resp.SimpleString "OK"

let get (key : string) : Resp.t =
  let value = Store.get key in
  match value with None -> Resp.Null | Some v -> resp_from_store v

let rpush (key : string) (rest : string list) : Resp.t =
  Store.mutate_list key (Lists.rpush rest)

let lpush (key : string) (rest : string list) =
  Store.mutate_list key (Lists.lpush rest)

let lrange (key : string) (from_idx : string) (to_idx : string) : Resp.t =
  Store.query_list key (Lists.lrange from_idx to_idx)

let llen (key : string) : Resp.t = Store.query_list key Lists.llen

let lpop (key : string) (count : int) : Resp.t =
  Store.mutate_list key (Lists.lpop count)

let blpop (key : string) (timeout : string) : Resp.t =
  let timeout =
    match Float.of_string_opt timeout with
    | Some v -> v
    | None -> Int.of_string timeout |> Float.of_int
  in
  Store.pop_list_or_wait key timeout (Lists.lpop 1)

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
  Store.mutate_stream key (Streams.xadd id rest)

let xrange (key : string) (from_id : string) (to_id : string) : Resp.t =
  Store.query_stream key (Streams.xrange from_id to_id)

let xread (key : string) (from_id : string) : Resp.t =
  Store.query_stream key (Streams.xread from_id)

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
  | "xrange", [ key; from_id; to_id ] -> xrange key from_id to_id
  | "xread", [ _; key; from_id ] -> xread key from_id
  | _ -> Resp.Null
