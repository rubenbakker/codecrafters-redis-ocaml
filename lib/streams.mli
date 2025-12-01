open Base

type t

val empty : unit -> t

val xadd :
  string -> string list -> int -> t option -> t option * Resp.t * Resp.t list

val xrange : string -> string -> t option -> Resp.t
val xread : string -> string -> t option -> Resp.t
val create : (string * (string * string) list) list -> t
val to_sexp : t -> Sexp.t
