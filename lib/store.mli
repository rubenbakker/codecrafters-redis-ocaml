type t =
  | StorageString of string
  | StorageList of Lists.t 
  | StorageStream of Streams.t

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit

val query_list : string -> (Lists.t option -> Resp.t) -> Resp.t
val mutate_list : string -> (Lists.t option -> (Lists.t option * Resp.t)) -> Resp.t
val pop_list_or_wait : string -> float -> (Lists.t option -> (Lists.t option * Resp.t)) -> Resp.t
val query_stream : string -> (Streams.t option -> Resp.t) -> Resp.t
val mutate_stream :
  string ->
  (Streams.t option -> Streams.t option * Resp.t) ->
  Resp.t


val start_gc : unit -> Thread.t
val start_expire_listeners : unit -> Thread.t
