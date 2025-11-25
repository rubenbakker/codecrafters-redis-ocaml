open Base
module StringMap = Stdlib.Map.Make (String)

(** storage value type *)
type t = StorageString of string | StorageList of string list

let repo : (t * Lifetime.t) StringMap.t ref = ref StringMap.empty
let repo_lock = Stdlib.Mutex.create ()
let unlock () = Stdlib.Mutex.unlock repo_lock
let lock () = Stdlib.Mutex.lock repo_lock

type listener = { lock : Stdlib.Mutex.t; condition : Stdlib.Condition.t }

let listeners : listener Queue.t StringMap.t ref = ref StringMap.empty

type pop_or_wait_result = WaitResult of listener | ValueResult of t

let protect fn =
  Stdlib.Fun.protect ~finally:unlock (fun () ->
      lock ();
      fn ())

let notify_first_listener key =
  match StringMap.find_opt key !listeners with
  | None -> ()
  | Some queue -> (
      if Queue.length queue > 0 then
        match Queue.dequeue queue with
        | None -> ()
        | Some listener -> Stdlib.Condition.signal listener.condition)

let set key value expires =
  protect (fun () ->
      repo :=
        StringMap.add key (value, Lifetime.to_abolute_expires expires) !repo;
      notify_first_listener key)

let get key =
  protect (fun () ->
      match StringMap.find_opt key !repo with
      | None -> None
      | Some (value, expiry) -> (
          match expiry with
          | Forever -> Some value
          | Expires e -> if e >= Lifetime.now () then Some value else None))

let pop_value key =
  let value =
    match StringMap.find_opt key !repo with
    | None -> None
    | Some (value, expiry) -> (
        match expiry with
        | Forever -> Some value
        | Expires e -> if e >= Lifetime.now () then Some value else None)
  in
  match value with
  | Some (StorageList (first :: rest)) ->
      repo := StringMap.add key (StorageList rest, Lifetime.Forever) !repo;
      Some (StorageString first)
  | _ -> None

let pop_or_wait key _timeout =
  let outcome =
    protect (fun () ->
        match pop_value key with
        | Some v -> ValueResult v
        | _ ->
            let condition = Stdlib.Condition.create () in
            let lock = Stdlib.Mutex.create () in
            let listener = { lock; condition } in
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
      pop_value key

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

let start_gc () = Thread.create remove_expired_entries_loop ()
