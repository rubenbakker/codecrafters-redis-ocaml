type t

val from_string : string -> int -> t
val to_string : t -> string
val command : string -> string * string list
val to_bulk_string : string -> string
val to_simple_string : string -> string
val null_string : string
