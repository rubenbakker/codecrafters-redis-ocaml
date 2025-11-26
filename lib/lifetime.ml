open Base

type t = Forever | Expires of int

let now () = Unix.gettimeofday () *. 1000.0 |> Int.of_float

let to_abolute_expires expires =
  match expires with Forever -> Forever | Expires ms -> Expires (now () + ms)

let create_expiry expiry_type expiry_value =
  let expiry_value = Int.of_string expiry_value in
  let expiry_value =
    match String.lowercase expiry_type with
    | "ex" -> expiry_value * 1000
    | "px" -> expiry_value
    | _ -> 0
  in
  Expires expiry_value

let has_expired current_time lifetime =
  match lifetime with Forever -> false | Expires time -> time < current_time

let create_expiry_with_ms millis = Expires millis
let create_expiry_with_s seconds = create_expiry_with_ms (seconds * 1000)
