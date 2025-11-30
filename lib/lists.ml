open! Base

type t = string list

let to_resp (existing_list : t) : Resp.t =
  Resp.RespList (List.map ~f:(fun str -> Resp.BulkString str) existing_list)

let normalize_lrange (len : int) (from_idx : int) (to_idx : int) :
    (int * int) option =
  let from_idx =
    if from_idx >= 0 then from_idx else Int.max (len + from_idx) 0
  in
  let to_idx = if to_idx >= 0 then to_idx else Int.max (len + to_idx) 0 in
  if from_idx >= 0 && from_idx < len then
    let to_idx = Int.min to_idx (len - 1) in
    let len = to_idx - from_idx + 1 in
    Some (from_idx, len)
  else None

let llen (existing_list : t option) : Resp.t =
  match existing_list with
  | Some l -> Resp.Integer (List.length l)
  | _ -> Resp.Integer 0

let lrange (from_idx : string) (to_idx : string) (existing_list : t option) :
    Resp.t =
  let from_idx = Int.of_string from_idx in
  let to_idx = Int.of_string to_idx in
  match existing_list with
  | Some l -> (
      match normalize_lrange (List.length l) from_idx to_idx with
      | Some (pos, len) ->
          Resp.RespList
            (l |> List.sub ~pos ~len
            |> List.map ~f:(fun str -> Resp.BulkString str))
      | None -> RespList [])
  | _ -> Resp.RespList []

let lpush (data : string list) (existing_list : t option) : t option * Resp.t =
  let values = List.rev data in
  let new_list =
    match existing_list with Some l -> values @ l | _ -> values
  in
  (Some new_list, Resp.Integer (List.length new_list))

let rpush (data : string list) (existing_list : t option) : t option * Resp.t =
  let new_list = match existing_list with Some l -> l @ data | _ -> data in
  (Some new_list, Resp.Integer (List.length new_list))

let lpop (count : int) (existing_list : t option) : t option * Resp.t =
  match existing_list with
  | Some existing_list -> (
      match existing_list with
      | first :: rest as l -> (
          match count with
          | 1 -> (Some rest, Resp.BulkString first)
          | _ ->
              let count = Int.min (List.length l) count in
              let result = List.take l count in
              let new_list =
                List.sub ~pos:count ~len:(List.length l - count) l
              in
              ( Some new_list,
                Resp.RespList
                  (List.map ~f:(fun str -> Resp.BulkString str) result) ))
      | _ -> (None, Resp.Null))
  | _ -> (None, Resp.Null)

let take (existing_list : t) (count : int) : Resp.t list * t =
  let count = min count (List.length existing_list) in
  let resps =
    List.take existing_list count |> List.map ~f:(fun s -> Resp.BulkString s)
  in
  ( resps,
    List.sub ~pos:count ~len:(List.length existing_list - count) existing_list
  )

let%test_unit "lrange positive idx" =
  [%test_eq: (int * int) option] (normalize_lrange 5 1 3) (Some (1, 3))

let%test_unit "lrange positive from_idx larger than list" =
  [%test_eq: (int * int) option] (normalize_lrange 5 1 7) (Some (1, 4))

let%test_unit "lrange positive from_idx larger than link" =
  [%test_eq: (int * int) option] (normalize_lrange 5 8 9) None

let%test_unit "lrange negative from_idx and to_idx" =
  [%test_eq: (int * int) option] (normalize_lrange 5 (-2) (-1)) (Some (3, 2))

let%test_unit "lrange negative from_idx and to_idx" =
  [%test_eq: (int * int) option] (normalize_lrange 5 (-7) 99) (Some (0, 5))
