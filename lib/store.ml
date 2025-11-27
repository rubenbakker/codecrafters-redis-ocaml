open Base
module StringMap = Stdlib.Map.Make (String)

(** storage value type *)
type t = StorageString of string | StorageList of string list

let repo : (t * Lifetime.t) StringMap.t ref = ref StringMap.empty
let repo_lock = Stdlib.Mutex.create ()
let unlock () = Stdlib.Mutex.unlock repo_lock
let lock () = Stdlib.Mutex.lock repo_lock

type listener = {
  lock : Stdlib.Mutex.t;
  condition : Stdlib.Condition.t;
  lifetime : Lifetime.t;
  mutable result : t option;
}

let listeners : listener Queue.t StringMap.t ref = ref StringMap.empty

type pop_or_wait_result = WaitResult of listener | ValueResult of t

let protect fn =
  Stdlib.Fun.protect ~finally:unlock (fun () ->
      lock ();
      fn ())

let notify_listeners key values =
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

let set_no_lock key value expires =
  repo := StringMap.add key (value, Lifetime.to_abolute_expires expires) !repo

let get_no_lock key =
  match StringMap.find_opt key !repo with
  | None -> None
  | Some (value, expiry) -> (
      match expiry with
      | Forever -> Some value
      | Expires e -> if e >= Lifetime.now () then Some value else None)

let set key value expires = protect (fun () -> set_no_lock key value expires)
let get key = protect (fun () -> get_no_lock key)

let rpush key values =
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

let lpush key rest =
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

let pop_value key =
  match get_no_lock key with
  | None -> None
  | Some value -> (
      match value with
      | StorageList (first :: rest) ->
          repo := StringMap.add key (StorageList rest, Lifetime.Forever) !repo;
          Some (StorageString first)
      | _ -> None)

let pop_or_wait key timeout =
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

let rec remove_expired_entries_loop () =
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

let rec expire_listeners () =
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

let start_gc () = Thread.create remove_expired_entries_loop ()
let start_expire_listeners () = Thread.create expire_listeners ()
