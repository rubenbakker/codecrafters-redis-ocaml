type t =
  | StorageString of string
  | StorageList of string list
  | StorageStream of Streams.t

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit
val rpush : string -> string list -> int
val lpush : string -> string list -> int
val pop_or_wait : string -> float -> t option
val start_gc : unit -> Thread.t
val start_expire_listeners : unit -> Thread.t
val query_stream : string -> (Streams.t option -> Resp.t) -> Resp.t

val mutate_stream :
  string ->
  (Streams.t option -> Streams.t option * Resp.t) ->
  Resp.t
