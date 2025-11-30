
type t

val xadd : string -> string list -> int -> t option -> t option * Resp.t * Resp.t list
val xrange : string -> string -> t option -> Resp.t
val xread : string -> string -> t option -> Resp.t
