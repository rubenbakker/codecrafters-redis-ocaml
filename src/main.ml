open Base
open Unix
open Lib

let empty_context () : Command.context_t =
  let options = Options.parse_options (Sys.get_argv ()) in
  { role = options.role; command_queue = None; post_process = Noop }

let post_process_command (context : Command.context_t)
    (client_socket : file_descr) : Command.context_t =
  ignore
    (match context.post_process with
    | RegisterSlave -> Master.register_slave client_socket
    | Mutation command -> Master.notify_slaves command
    | Noop -> ());
  { context with post_process = Noop }

let rec process_client client_socket (context : Command.context_t) =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result, context = buf |> Bytes.to_string |> Command.process context in
      let result = Resp.to_string result in
      let _ =
        write client_socket (Bytes.of_string result) 0 (String.length result)
      in
      let context = post_process_command context client_socket in
      process_client client_socket context
  with
  | Unix_error (ECONNRESET, _, _) ->
      Stdlib.print_endline "Error: unix error - reset connection"
  | End_of_file -> Stdlib.print_endline "Error: end of file"
  | _ -> Stdlib.print_endline "Error: Unknown"

let send_to_master sock (payload : Resp.t) : Resp.t =
  let payload = payload |> Resp.to_string |> Bytes.of_string in
  ignore (write sock payload 0 (Bytes.length payload));
  let buf = Bytes.create 512 in
  ignore (Unix.read sock buf 0 512);
  Resp.from_string (Bytes.to_string buf) 0

let rec process_slave client_socket (context : Command.context_t) : unit =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    let command_string = Bytes.to_string buf in
    Stdlib.print_endline "Slave received";
    Stdlib.print_endline command_string;
    if bytes_read > 0 then
      ignore
        ((match Command.parse_command_line command_string with
         | "replconf", [ "GETACK"; "*" ] ->
             let result =
               ("REPLCONF", [ "ACK"; "0" ]) |> Command.resp_from_command
             in
             Stdlib.print_endline "sneding result to master";
             ignore (send_to_master client_socket result)
         | _ -> ignore (Command.process context command_string));
         process_slave client_socket context)
    else Stdlib.print_endline "Error: No bytes received, existing slave sync"
  with
  | Unix_error (ECONNRESET, _, _) ->
      Stdlib.print_endline "Error: unix error - reset connection"
  | End_of_file -> Stdlib.print_endline "Error: end of file"
  | _ -> Stdlib.print_endline "Error: Unknown"

let run_and_close_client client_socket =
  let finally () = close client_socket in
  let work () = process_client client_socket (empty_context ()) in
  Stdlib.Fun.protect ~finally work

let rec accept_loop server_socket threads =
  let client_socket, _ = accept server_socket in
  let thread = Thread.create run_and_close_client client_socket in
  accept_loop server_socket (thread :: threads)

let create_command (args : string list) : Resp.t =
  args |> List.map ~f:(fun a -> Resp.BulkString a) |> fun l -> Resp.RespList l

let init_slave (host : string) (port : int) (slave_port : int) =
  let hostaddr =
    try inet_addr_of_string host
    with Failure _ -> (
      try (gethostbyname host).h_addr_list.(0) with _ -> assert false)
  in
  let sock = socket PF_INET SOCK_STREAM 0 in
  connect sock (ADDR_INET (hostaddr, port));
  ignore (create_command [ "PING" ] |> send_to_master sock);
  ignore
    (create_command [ "REPLCONF"; "listening-port"; Int.to_string slave_port ]
    |> send_to_master sock);
  ignore (create_command [ "REPLCONF"; "capa"; "psync2" ] |> send_to_master sock);
  ignore (create_command [ "PSYNC"; "?"; "-1" ] |> send_to_master sock);
  ignore (Thread.create (fun () -> process_slave sock (empty_context ())) ())

let () =
  (* Create a TCP server socket *)
  let options = Options.parse_options (Sys.get_argv ()) in
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  Stdlib.flush Stdlib.stderr;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", options.port));
  listen server_socket 10;
  ignore (Store.start_gc ());
  ignore (Store.start_expire_listeners ());
  (match options.role with
  | Slave (host, port) -> init_slave host port options.port
  | Master -> ());
  try accept_loop server_socket []
  with e ->
    close server_socket;
    raise e
