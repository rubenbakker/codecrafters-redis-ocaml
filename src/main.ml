open Unix
module StringMap = Stdlib.Map.Make (String)

let process_command repo str =
  let command = Resp.command str in
  match command with
  | "ping", [] -> Resp.to_simple_string "PONG"
  | "set", [ key; value ] ->
      repo := StringMap.add key value !repo;
      Resp.to_simple_string "OK"
  | "get", [ key ] -> (
      let value = StringMap.find_opt key !repo in
      match value with
      | None -> Resp.null_string
      | Some v -> Resp.to_bulk_string v)
  | "echo", [ message ] -> Resp.to_bulk_string message
  | _ -> Resp.to_simple_string "PONG"

let rec process_client repo client_socket =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result = buf |> String.of_bytes |> process_command repo in
      let _ =
        write client_socket (Bytes.of_string result) 0 (String.length result)
      in
      process_client repo client_socket
    else ()
  with
  | Unix_error (ECONNRESET, _, _) -> ()
  | End_of_file -> ()

let run_and_close_client (repo, client_socket) =
  let finally () = close client_socket in
  let work () = process_client repo client_socket in
  Fun.protect ~finally work

let rec accept_loop server_socket threads repo =
  let client_socket, _ = accept server_socket in
  let thread = Thread.create run_and_close_client (repo, client_socket) in
  accept_loop server_socket (thread :: threads) repo

let () =
  (* Create a TCP server socket *)
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 10;
  let repo = ref StringMap.empty in
  try accept_loop server_socket [] repo
  with e ->
    close server_socket;
    raise e
