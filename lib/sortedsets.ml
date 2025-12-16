open Base

type entry_t = { value : string; score : float } [@@deriving sexp]
type entries_t = entry_t list [@@deriving sexp]
type t = (string, float, String.comparator_witness) Map.t

let empty () : t = Map.empty (module String)

let add ~(value : string) ~(score : float) (set : t) =
  Map.set set ~key:value ~data:score

let remove ~(value : string) (set : t) = Map.remove set value

let sorted_entries (set : t) : entry_t list =
  Map.to_alist set
  |> List.map ~f:(fun (value, score) -> { value; score })
  |> List.sort ~compare:(fun left right -> Float.compare left.score right.score)

let zadd ~(value : string) ~(score : float) (_listener_count : int)
    (set : t option) : t Storeop.mutation_result =
  let set = match set with Some set -> set | None -> empty () in
  let previous_size = Map.length set in
  let result_set = add ~value ~score set in
  {
    store = Some result_set;
    return = Resp.Integer (Map.length result_set - previous_size);
    notify_with = [];
  }
