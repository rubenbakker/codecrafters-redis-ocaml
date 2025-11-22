open Unix

let () =
  (* Create a TCP server socket *)
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 1;

  (* Uncomment the code below to pass the first stage *)
  let data = "+PONG\r\n" in
  let buf = Bytes.create 32 in
  let client_socket, _ = accept server_socket in
  while true do
    let _ = read client_socket buf 0 16 in
    let _ = write client_socket (Bytes.of_string data) 0 (String.length data) in
    ()
  done;
  close client_socket;
  close server_socket
