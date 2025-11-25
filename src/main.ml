open Unix
open Base
open Lib

let process_command str =
  let command = Resp.command str in
  match command with
  | "ping", [] -> Resp.SimpleString "PONG"
  | "set", [ key; value ] ->
      Store.set key (Store.String value) Lifetime.Forever;
      Resp.SimpleString "OK"
  | "set", [ key; value; expiry_type; expiry_value ] ->
      Store.set key (Store.String value)
        (Lifetime.create_expiry expiry_type expiry_value);
      Resp.SimpleString "OK"
  | "get", [ key ] -> (
      let value = Store.get key in
      match value with None -> Resp.Null | Some v -> Resp.from_store v)
  | "rpush", key :: rest ->
      let exiting_list = Store.get key in
      let new_list =
        match exiting_list with Some (Store.List l) -> l @ rest | _ -> rest
      in
      Store.set key (Store.List new_list) Lifetime.Forever;
      Resp.Integer (List.length new_list)
  | "lrange", [ key; from_idx; to_idx ] -> (
      let pos = Int.of_string from_idx in
      let to_idx = Int.of_string to_idx in
      match Store.get key with
      | Some (Store.List l) when List.length l > pos ->
          let to_idx = Int.min to_idx (List.length l - 1) in
          let len = to_idx - pos + 1 in
          Stdlib.Printf.eprintf "pos: %d, to_idx: %d, len: %d" pos to_idx len;
          Resp.RespList
            (l |> List.sub ~pos ~len
            |> List.map ~f:(fun str -> Resp.BulkString str))
      | _ -> Resp.RespList [])
  | "echo", [ message ] -> Resp.BulkString message
  | _ -> Resp.Null

let rec process_client client_socket =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result =
        buf |> Stdlib.String.of_bytes |> process_command |> Resp.to_string
      in
      let _ =
        write client_socket (Bytes.of_string result) 0 (String.length result)
      in
      process_client client_socket
    else ()
  with
  | Unix_error (ECONNRESET, _, _) -> ()
  | End_of_file -> ()

let run_and_close_client client_socket =
  let finally () = close client_socket in
  let work () = process_client client_socket in
  Stdlib.Fun.protect ~finally work

let rec accept_loop server_socket threads =
  let client_socket, _ = accept server_socket in
  let thread = Thread.create run_and_close_client client_socket in
  accept_loop server_socket (thread :: threads)

let () =
  (* Create a TCP server socket *)
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 10;
  let _ = Store.start_gc () in
  try accept_loop server_socket []
  with e ->
    close server_socket;
    raise e
