type stream_entry = {
  id: string;
  data: (string * string) list;
}
(** storage value type *)
type t =
  | StorageString of string
  | StorageList of string list
  | StorageStream of stream_entry list

val get : string -> t option
val set : string -> t -> Lifetime.t -> unit
val rpush : string -> string list -> int
val lpush : string -> string list -> int
val pop_or_wait : string -> float -> t option
val xadd : string -> string -> string list -> string
val start_gc : unit -> Thread.t
val start_expire_listeners : unit -> Thread.t
