open Unix
open Base
module StringMap = Stdlib.Map.Make (String)

let now () = Unix.gettimeofday () *. 1000.0 |> Stdlib.int_of_float

type lifetime = Forever | Expires of int

let to_abolute_expires expires =
  match expires with Forever -> Forever | Expires ms -> Expires (now () + ms)

let repo_set key value expires repo =
  StringMap.add key (value, to_abolute_expires expires) !repo

let repo_get : string -> (string * lifetime) StringMap.t ref -> string option =
 fun key repo ->
  match StringMap.find_opt key !repo with
  | None -> None
  | Some (value, expiry) -> (
      match expiry with
      | Forever -> Some value
      | Expires e -> if e >= now () then Some value else None)

let create_expiry expiry_type expiry_value =
  let expiry_value = Stdlib.int_of_string expiry_value in
  let expiry_value =
    match String.lowercase expiry_type with
    | "ex" -> expiry_value * 1000
    | "px" -> expiry_value
    | _ -> 0
  in
  Expires expiry_value

let process_command repo str =
  let command = Resp.command str in
  match command with
  | "ping", [] -> Resp.to_simple_string "PONG"
  | "set", [ key; value ] ->
      repo := repo_set key value Forever repo;
      Resp.to_simple_string "OK"
  | "set", [ key; value; expiry_type; expiry_value ] ->
      repo := repo_set key value (create_expiry expiry_type expiry_value) repo;
      Resp.to_simple_string "OK"
  | "get", [ key ] -> (
      let value = repo_get key repo in
      match value with
      | None -> Resp.null_string
      | Some v -> Resp.to_bulk_string v)
  | "echo", [ message ] -> Resp.to_bulk_string message
  | _ -> Resp.null_string

let rec process_client repo client_socket =
  try
    let buf = Bytes.create 2024 in
    let bytes_read = Unix.read client_socket buf 0 2024 in
    if bytes_read > 0 then
      let result = buf |> Stdlib.String.of_bytes |> process_command repo in
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
  Stdlib.Fun.protect ~finally work

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
