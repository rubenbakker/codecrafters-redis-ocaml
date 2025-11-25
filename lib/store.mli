type t = StorageString of string | StorageList of string list

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit
val pop_or_wait : string -> int -> t option
val start_gc : unit -> Thread.t
