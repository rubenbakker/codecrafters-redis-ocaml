open Unix
open Base
open Lib

let process_command str =
  let command = Resp.command str in
  match command with
  | "ping", [] -> Resp.to_simple_string "PONG"
  | "set", [ key; value ] ->
      Store.set key (Store.String value) Lifetime.Forever;
      Resp.to_simple_string "OK"
  | "set", [ key; value; expiry_type; expiry_value ] ->
      Store.set key (Store.String value)
        (Lifetime.create_expiry expiry_type expiry_value);
      Resp.to_simple_string "OK"
  | "get", [ key ] -> (
      let value = Store.get key in
      match value with
      | None -> Resp.null_string
      | Some v -> Resp.from_store v |> Resp.to_string)
  | "rpush", [ key; value ] ->
      let exiting_list = Store.get key in
      let new_list =
        match exiting_list with
        | Some (Store.List l) -> l @ [ value ]
        | _ -> [ value ]
      in
      Store.set key (Store.List new_list) Lifetime.Forever;
      Resp.to_integer_string (List.length new_list)
  | "echo", [ message ] -> Resp.to_bulk_string message
  | _ -> Resp.null_string

let rec process_client client_socket =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result = buf |> Stdlib.String.of_bytes |> process_command in
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
