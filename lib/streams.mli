
type t

val xadd : string -> string list -> t option -> t option * Resp.t
val xrange : string -> string -> t option -> Resp.t
val xread : string -> t option -> Resp.t
