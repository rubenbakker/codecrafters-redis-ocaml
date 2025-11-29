(** storage value type *)
type t =
  | StorageString of string
  | StorageList of string list
  | StorageStream of Streams.entry_t list

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit
val rpush : string -> string list -> int
val lpush : string -> string list -> int
val pop_or_wait : string -> float -> t option

val xadd :
  string ->
  string ->
  string list ->
  (string * Streams.entry_t list, string) Result.t

val xrange :
  string -> string -> string -> (Streams.entry_t list, string) Result.t

val start_gc : unit -> Thread.t
val start_expire_listeners : unit -> Thread.t
