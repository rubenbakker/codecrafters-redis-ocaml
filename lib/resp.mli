type t
val from_string : string -> int -> t 
val to_string : t -> string 
val command : string -> string * string
val string_to_resp : string -> string
