open Base
open Unix
open Lib

let rec process_client client_socket (command_queue : Command.command_queue_t) =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result, command_queue =
        buf |> Bytes.to_string |> Command.process command_queue
      in
      let result = Resp.to_string result in
      let _ =
        write client_socket (Bytes.of_string result) 0 (String.length result)
      in
      process_client client_socket command_queue
  with
  | Unix_error (ECONNRESET, _, _) ->
      Stdlib.print_endline "Error: unix error - reset connection"
  | End_of_file -> Stdlib.print_endline "Error: end of file"
  | _ -> Stdlib.print_endline "Error: Unknown"

let run_and_close_client client_socket =
  let finally () = close client_socket in
  let work () = process_client client_socket (Command.empty_command_queue ()) in
  Stdlib.Fun.protect ~finally work

let rec accept_loop server_socket threads =
  let client_socket, _ = accept server_socket in
  let thread = Thread.create run_and_close_client client_socket in
  accept_loop server_socket (thread :: threads)

let send_resp_to_socket sock (payload : Resp.t) =
  let payload = payload |> Resp.to_string |> Bytes.of_string in
  ignore (write sock payload 0 (Bytes.length payload));
  ignore (Unix.read sock (Bytes.create 512) 0 512)

let init_slave (host : string) (port : int) =
  let hostaddr =
    try inet_addr_of_string host
    with Failure _ -> (
      try (gethostbyname host).h_addr_list.(0) with _ -> assert false)
  in
  let sock = socket PF_INET SOCK_STREAM 0 in
  connect sock (ADDR_INET (hostaddr, port));
  Resp.RespList [ Resp.BulkString "PING" ] |> send_resp_to_socket sock;
  Resp.RespList
    [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "listening-port";
      Resp.BulkString (Int.to_string port);
    ]
  |> send_resp_to_socket sock;
  Resp.RespList
    [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "capa";
      Resp.BulkString "psync2";
    ]
  |> send_resp_to_socket sock;

  ignore (close sock)

let () =
  (* Create a TCP server socket *)
  let options = Options.parse_options (Sys.get_argv ()) in
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", options.port));
  listen server_socket 10;
  ignore (Store.start_gc ());
  ignore (Store.start_expire_listeners ());
  (match options.role with
  | Slave (host, port) -> init_slave host port
  | Master -> ());
  try accept_loop server_socket []
  with e ->
    close server_socket;
    raise e
