open Unix

let process_command str =
  let command, arg = Resp.command str in
  match command with
  | "ping" -> "+PONG\r\n"
  | "echo" -> Resp.string_to_resp arg
  | _ -> "+PONG\r\n"

let rec process_client client_socket =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result = buf |> String.of_bytes |> process_command in
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
  Fun.protect ~finally work

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

  try accept_loop server_socket []
  with e ->
    close server_socket;
    raise e
