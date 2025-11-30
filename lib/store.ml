open Base
module StringMap = Stdlib.Map.Make (String)

(** storage value type *)
type t =
  | StorageString of string
  | StorageList of Lists.t
  | StorageStream of Streams.t

(** storage data stored as repo *)
let repo : (t * Lifetime.t) StringMap.t ref = ref StringMap.empty

(** lock when accessing the repo *)
let repo_lock = Stdlib.Mutex.create ()

(** lock the repo for access *)
let unlock () : unit = Stdlib.Mutex.unlock repo_lock

(** unlock the repo after access *)
let lock () : unit = Stdlib.Mutex.lock repo_lock

(* Listener type to be notified when an element becomes available *)
type listener = {
  lock : Stdlib.Mutex.t;
  condition : Stdlib.Condition.t;
  lifetime : Lifetime.t;
  mutable result : Resp.t option;
}

(** module global storage for all listeners *)
let listeners : listener Queue.t StringMap.t ref = ref StringMap.empty

type pop_or_wait_result = WaitResult of listener | ValueResult of Resp.t

let protect fn =
  Stdlib.Fun.protect ~finally:unlock (fun () ->
      lock ();
      fn ())

let set_no_lock (key : string) (value : t) (expires : Lifetime.t) : unit =
  repo := StringMap.add key (value, Lifetime.to_abolute_expires expires) !repo

let notify_listeners (key : string) (l : Lists.t) : unit =
  match StringMap.find_opt key !listeners with
  | None -> ()
  | Some queue ->
      let resp_items, new_list = Lists.take l (Queue.length queue) in
      set_no_lock key (StorageList new_list) Lifetime.Forever;
      resp_items
      |> List.iter ~f:(fun value ->
             match Queue.dequeue queue with
             | None -> ()
             | Some listener ->
                 listener.result <- Some value;
                 Stdlib.Condition.signal listener.condition)

let get_no_lock (key : string) : t option =
  match StringMap.find_opt key !repo with
  | None -> None
  | Some (value, expiry) -> (
      match expiry with
      | Forever -> Some value
      | Expires e -> if e >= Lifetime.now () then Some value else None)

let set (key : string) (value : t) (expires : Lifetime.t) : unit =
  protect (fun () -> set_no_lock key value expires)

let get (key : string) : t option = protect (fun () -> get_no_lock key)

let as_list_option (storage_value : t option) : Lists.t option =
  match storage_value with Some (StorageList l) -> Some l | _ -> None

let query_list (key : string) (fn : Lists.t option -> Resp.t) : Resp.t =
  protect (fun () -> get_no_lock key |> as_list_option |> fn)

let mutate_list (key : string) (fn : Lists.t option -> Lists.t option * Resp.t)
    : Resp.t =
  protect (fun () ->
      let exiting_list = get_no_lock key |> as_list_option in
      let new_list, resp = fn exiting_list in
      match new_list with
      | Some l ->
          set_no_lock key (StorageList l) Lifetime.Forever;
          notify_listeners key l;
          resp
      | None -> resp)

let pop_list_or_wait (key : string) (timeout : float)
    (fn : Lists.t option -> Lists.t option * Resp.t) : Resp.t =
  let outcome =
    protect (fun () ->
        let exiting_list = get_no_lock key |> as_list_option in
        let data, resp = fn exiting_list in
        match data with
        | Some l ->
            set_no_lock key (StorageList l) Lifetime.Forever;
            ValueResult (Resp.RespList [ Resp.BulkString key; resp ])
        | _ ->
            let condition = Stdlib.Condition.create () in
            let lock = Stdlib.Mutex.create () in
            let lifetime =
              match timeout with
              | 0.0 -> Lifetime.Forever
              | _ ->
                  Lifetime.create_expiry_with_s timeout
                  |> Lifetime.to_abolute_expires
            in
            let listener = { lock; condition; lifetime; result = None } in
            Stdlib.Mutex.lock lock;
            let queue =
              match StringMap.find_opt key !listeners with
              | None ->
                  let q : listener Queue.t = Queue.create () in
                  listeners := StringMap.add key q !listeners;
                  q
              | Some q -> q
            in
            Queue.enqueue queue listener;
            WaitResult listener)
  in
  match outcome with
  | ValueResult v -> v
  | WaitResult listener ->
      Stdlib.Condition.wait listener.condition listener.lock;
      protect (fun () ->
          match listener.result with
          | Some resp -> Resp.RespList [ Resp.BulkString key; resp ]
          | None -> Resp.NullArray)

let as_stream_option (storage_value : t option) : Streams.t option =
  match storage_value with
  | Some (StorageStream stream) -> Some stream
  | _ -> None

let query_stream (key : string) (fn : Streams.t option -> Resp.t) : Resp.t =
  protect (fun () -> get_no_lock key |> as_stream_option |> fn)

let mutate_stream (key : string)
    (fn : Streams.t option -> Streams.t option * Resp.t) : Resp.t =
  protect (fun () ->
      let stream, resp = get_no_lock key |> as_stream_option |> fn in
      (match stream with
      | Some stream -> set_no_lock key (StorageStream stream) Lifetime.Forever
      | None -> ());
      resp)

let rec remove_expired_entries_loop () : unit =
  protect (fun () ->
      let current_time = Lifetime.now () in
      let expired_entries =
        StringMap.filter
          (fun _ value ->
            match value with
            | _, Lifetime.Forever -> false
            | _, Lifetime.Expires e -> e < current_time)
          !repo
      in
      StringMap.iter
        (fun key _ -> repo := StringMap.remove key !repo)
        expired_entries);
  Thread.delay 10.0;
  remove_expired_entries_loop ()

let rec expire_listeners () : unit =
  let _ =
    protect (fun () ->
        let current_time = Lifetime.now () in
        StringMap.iter
          (fun key queue ->
            Queue.filter queue ~f:(fun listener ->
                Lifetime.has_expired current_time listener.lifetime)
            |> Queue.iter ~f:(fun listener ->
                   Stdlib.Condition.signal listener.condition);
            let new_queue =
              Queue.filter queue ~f:(fun listener ->
                  not (Lifetime.has_expired current_time listener.lifetime))
            in
            listeners := StringMap.add key new_queue !listeners)
          !listeners)
  in
  Thread.delay 0.1;
  expire_listeners ()

let start_gc () : Thread.t = Thread.create remove_expired_entries_loop ()
let start_expire_listeners () : Thread.t = Thread.create expire_listeners ()
