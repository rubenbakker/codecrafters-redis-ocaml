
type t

val to_resp: t -> Resp.t
val llen: t option -> Resp.t
val lrange: string -> string -> t option -> Resp.t
val lpush : string list -> t option -> t option * Resp.t
val rpush : string list -> t option -> t option * Resp.t
val lpop : int -> t option -> t option * Resp.t
val take : t -> int -> Resp.t list * t
