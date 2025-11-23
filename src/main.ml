open Unix

let rec process_client client_socket =
  let data = "+PONG\r\n" in
  let buf = Bytes.create 32 in
  let _ = read client_socket buf 0 16 in
  let _ = write client_socket (Bytes.of_string data) 0 (String.length data) in
  process_client client_socket

let run_and_close_client client_socket =
  let finally () = close client_socket in
  let work () = process_client client_socket in
  Fun.protect ~finally work

let rec accept_loop server_socket threads =
  let client_socket, _ = accept server_socket in
  accept_loop server_socket
    (Thread.create run_and_close_client client_socket :: threads)

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
