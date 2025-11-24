open Base

type t = Forever | Expires of int

let now () = Unix.gettimeofday () *. 1000.0 |> Stdlib.int_of_float

let to_abolute_expires expires =
  match expires with Forever -> Forever | Expires ms -> Expires (now () + ms)

let create_expiry expiry_type expiry_value =
  let expiry_value = Stdlib.int_of_string expiry_value in
  let expiry_value =
    match String.lowercase expiry_type with
    | "ex" -> expiry_value * 1000
    | "px" -> expiry_value
    | _ -> 0
  in
  Expires expiry_value
