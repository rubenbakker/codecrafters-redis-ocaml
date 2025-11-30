
type t

val to_resp: t -> Resp.t
val llen: t option -> Resp.t
val lrange: string -> string -> t option -> Resp.t
val lpush : string list -> int -> t option -> t option * Resp.t * Resp.t list
val rpush : string list -> int -> t option -> t option * Resp.t * Resp.t list
val lpop : int -> int -> t option -> t option * Resp.t * Resp.t list
