open Unix

let process_client client_socket =
  let data = "+PONG\r\n" in
  let buf = Bytes.create 32 in
  let bytes_sent = ref 1 in
  while !bytes_sent > 0 do
    let _ = read client_socket buf 0 16 in
    bytes_sent :=
      write client_socket (Bytes.of_string data) 0 (String.length data);
    Printf.eprintf "bytes_sent %d" !bytes_sent
  done;
  close client_socket

let () =
  (* Create a TCP server socket *)
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  let client_threads = ref [] in
  listen server_socket 1;

  (* Uncomment the code below to pass the first stage *)
  while true do
    let client_socket, _ = accept server_socket in
    Printf.eprintf "starting new thread %d"
      (List.length !client_threads)
      client_threads
    := Thread.create process_client client_socket :: !client_threads
  done
(* close server_socket *)
