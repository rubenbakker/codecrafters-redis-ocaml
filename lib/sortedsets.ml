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
  |> List.sort ~compare:(fun left right ->
      match Float.compare left.score right.score with
      | 0 -> String.compare left.value right.value
      | result -> result)

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

let zrank ~(value : string) (set : t option) : Storeop.query_result =
  let set = match set with Some set -> set | None -> empty () in
  match
    sorted_entries set
    |> List.map ~f:(fun entry -> (entry.value, entry.score))
    |> List.findi ~f:(fun _ (v, _) -> String.(value = v))
  with
  | Some (idx, _) -> Storeop.Value (Resp.Integer idx)
  | _ -> Storeop.Value Resp.Null

let zrange ~(from_idx : int) ~(to_idx : int) (set : t option) :
    Storeop.query_result =
  let set = match set with Some set -> set | None -> empty () in
  let to_idx = min to_idx (Map.length set) in
  match
    sorted_entries set
    |> List.filteri ~f:(fun idx _ -> idx >= from_idx && idx <= to_idx)
    |> List.map ~f:(fun entry -> entry.value)
  with
  | [] -> Storeop.Value Resp.NullArray
  | result ->
      Storeop.Value
        (Resp.RespList (List.map ~f:(fun value -> Resp.BulkString value) result))
