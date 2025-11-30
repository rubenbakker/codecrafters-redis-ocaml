type t =
  | StorageString of string
  | StorageList of Lists.t 
  | StorageStream of Streams.t

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit


val query : string -> (t option -> 'a option) -> ('a option -> Resp.t) -> Resp.t 
val pop_list_or_wait : string -> float -> (int -> Lists.t option -> (Lists.t option * Resp.t * Resp.t list)) -> Resp.t
val mutate :
  string ->
  (t option -> 'a option) ->
  ('a option -> t option) ->
  (int -> 'a option -> ('a option * Resp.t * Resp.t list)) ->
  Resp.t


val start_gc : unit -> Thread.t
val start_expire_listeners : unit -> Thread.t
