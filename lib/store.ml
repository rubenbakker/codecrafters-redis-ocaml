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

type wait_result = WaitResult of listener | ValueResult of Resp.t

let protect fn =
  Stdlib.Fun.protect ~finally:unlock (fun () ->
      lock ();
      fn ())

let set_no_lock (key : string) (value : t) (expires : Lifetime.t) : unit =
  repo := StringMap.add key (value, Lifetime.to_abolute_expires expires) !repo

let notify_listeners (listener_queue : listener Queue.t)
    (resp_items : Resp.t list) : unit =
  resp_items
  |> List.iter ~f:(fun value ->
      match Queue.dequeue listener_queue with
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

let convert_to_list (storage_value : t option) : Lists.t option =
  match storage_value with Some (StorageList l) -> Some l | _ -> None

let pop_list_or_wait (key : string) (timeout : float)
    (fn : int -> Lists.t option -> Lists.t Storeop.mutation_result) : Resp.t =
  let outcome =
    protect (fun () ->
        let exiting_list = get_no_lock key |> convert_to_list in
        let result = fn 0 exiting_list in
        match result.store with
        | Some l ->
            set_no_lock key (StorageList l) Lifetime.Forever;
            ValueResult (Resp.RespList [ Resp.BulkString key; result.return ])
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

let query (key : string) (convert : t option -> 'a option)
    (query : 'a option -> Storeop.query_result) : Resp.t =
  protect (fun () ->
      let result = get_no_lock key |> convert |> query in
      result.return)

let mutate (key : string) (from_store : 't option -> 'a option)
    (to_store : 'a option -> t option)
    (fn : int -> 'a option -> 'a Storeop.mutation_result) : Resp.t =
  protect (fun () ->
      let listener_queue = StringMap.find_opt key !listeners in
      let queue_length =
        match listener_queue with Some q -> Queue.length q | None -> 0
      in
      let result = get_no_lock key |> from_store |> fn queue_length in
      (match to_store result.store with
      | Some store_data -> (
          set_no_lock key store_data Lifetime.Forever;
          match listener_queue with
          | Some lq -> notify_listeners lq result.notify_with
          | None -> ())
      | None -> ());
      result.return)

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
