type id_t = { millis : int; sequence : int }
type entry_t = { id : id_t; data : (string * string) list }

val add_entry_to_stream :
  string ->
  (string * string) list ->
  entry_t list ->
  (string * entry_t list, string) Result.t
