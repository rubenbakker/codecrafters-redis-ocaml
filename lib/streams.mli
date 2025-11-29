type id_t = { millis : int; sequence : int }
type entry_t = { id : id_t; data : (string * string) list }

(** Add a new entry to an existing redis stream *)

val add_entry_to_stream :
  string ->
  (string * string) list ->
  entry_t list ->
  (string * entry_t list, string) Result.t

(** Get range of entries *)

val id_to_string : id_t -> string
val xrange : string -> string -> entry_t list -> entry_t list
val get_entry_id : entry_t -> id_t
val get_entry_data : entry_t -> (string * string) list
