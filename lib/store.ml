open Base
module StringMap = Stdlib.Map.Make (String)

(** storage value type *)
type t =
  | StorageString of string
  | StorageList of string list
  | StorageStream of Streams.entry_t list

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
  mutable result : t option;
}

(** module global storage for all listeners *)
let listeners : listener Queue.t StringMap.t ref = ref StringMap.empty

type pop_or_wait_result = WaitResult of listener | ValueResult of t

let protect fn =
  Stdlib.Fun.protect ~finally:unlock (fun () ->
      lock ();
      fn ())

let notify_listeners (key : string) (values : string list) : unit =
  match StringMap.find_opt key !listeners with
  | None -> ()
  | Some queue ->
      let count = Int.min (Queue.length queue) (List.length values) in
      List.take values count
      |> List.iter ~f:(fun value ->
          match Queue.dequeue queue with
          | None -> ()
          | Some listener ->
              listener.result <- Some (StorageString value);
              Stdlib.Condition.signal listener.condition)

let set_no_lock (key : string) (value : t) (expires : Lifetime.t) : unit =
  repo := StringMap.add key (value, Lifetime.to_abolute_expires expires) !repo

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

let rpush (key : string) (values : string list) : int =
  protect (fun () ->
      let exiting_list = get_no_lock key in
      let new_list =
        match exiting_list with
        | Some (StorageList l) -> l @ values
        | _ -> values
      in
      set_no_lock key (StorageList new_list) Lifetime.Forever;
      notify_listeners key new_list;
      List.length new_list)

let lpush (key : string) (rest : string list) : int =
  protect (fun () ->
      let values = List.rev rest in
      let exiting_list = get_no_lock key in
      let new_list =
        match exiting_list with
        | Some (StorageList l) -> values @ l
        | _ -> values
      in
      set_no_lock key (StorageList new_list) Lifetime.Forever;
      notify_listeners key new_list;
      List.length new_list)

let pop_value (key : string) : t option =
  match get_no_lock key with
  | None -> None
  | Some value -> (
      match value with
      | StorageList (first :: rest) ->
          repo := StringMap.add key (StorageList rest, Lifetime.Forever) !repo;
          Some (StorageString first)
      | _ -> None)

let pop_or_wait (key : string) (timeout : float) : t option =
  let outcome =
    protect (fun () ->
        match pop_value key with
        | Some v -> ValueResult v
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
  | ValueResult v -> Some v
  | WaitResult listener ->
      Stdlib.Condition.wait listener.condition listener.lock;
      protect (fun () -> listener.result)

let xadd (key : string) (id : string) (rest : string list) :
    (string * Streams.entry_t list, string) Result.t =
  let stream =
    match get_no_lock key with
    | Some (StorageStream stream) -> stream
    | None -> []
    | _ -> []
  in
  match Streams.add_entry_to_stream id (Listutils.make_pairs rest) stream with
  | Ok (id, stream) ->
      set_no_lock key (StorageStream stream) Lifetime.Forever;
      Ok (id, stream)
  | Error err -> Error err

let xrange (key : string) (from_id : string) (to_id : string) :
    (Streams.entry_t list, string) Result.t =
  let stream =
    match get_no_lock key with
    | Some (StorageStream stream) -> stream
    | None -> []
    | _ -> []
  in
  Ok (Streams.xrange from_id to_id stream)

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
