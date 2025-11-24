open Base
module StringMap = Stdlib.Map.Make (String)

(** storage value type *)
type t = String of string | List of string list

let repo : (t * Lifetime.t) StringMap.t ref = ref StringMap.empty

let set key value expires =
  repo := StringMap.add key (value, Lifetime.to_abolute_expires expires) !repo

let get key =
  match StringMap.find_opt key !repo with
  | None -> None
  | Some (value, expiry) -> (
      match expiry with
      | Forever -> Some value
      | Expires e -> if e >= Lifetime.now () then Some value else None)

let rec remove_expired_entries_loop () =
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
    expired_entries;
  Thread.delay 10.0;
  remove_expired_entries_loop ()

let start_gc () = Thread.create remove_expired_entries_loop ()
