type t = Forever | Expires of int

val to_abolute_expires : t -> t
val now : unit -> int
val create_expiry : string -> string -> t
