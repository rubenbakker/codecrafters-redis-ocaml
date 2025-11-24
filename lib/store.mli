type t = String of string | List of string list

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit
val start_gc : unit -> Thread.t
