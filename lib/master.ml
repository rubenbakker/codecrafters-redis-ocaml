type slave_t = { socket : Unix.file_descr }
type t = { slaves : slave_t list }

let state_lock = Stdlib.Mutex.create ()

(** lock the repo for access *)
let unlock () : unit = Stdlib.Mutex.unlock state_lock

(** unlock the repo after access *)
let lock () : unit = Stdlib.Mutex.lock state_lock

let protect fn =
  Stdlib.Fun.protect ~finally:unlock (fun () ->
      lock ();
      fn ())

let state : Unix.file_descr list ref = ref []

let send_to_slave sock (payload : Resp.t) : Resp.t =
  let payload = payload |> Resp.to_string |> Bytes.of_string in
  ignore (Unix.write sock payload 0 (Bytes.length payload));
  let buf = Bytes.create 512 in
  ignore (Unix.read sock buf 0 512);
  Resp.from_string (Bytes.to_string buf) 0

let register_slave (socket : Unix.file_descr) (rdb : Resp.t option) : unit =
  protect (fun () -> state := socket :: !state);
  Stdlib.print_endline "before rdb";
  (match rdb with
  | Some rdb ->
      let rdb_string = Resp.to_string rdb in
      ignore
        (Unix.write socket
           (Bytes.of_string rdb_string)
           0 (String.length rdb_string))
  | None -> ());
  Stdlib.print_endline "after rdb";
  let result =
    ("REPLCONF", [ "GETACK"; "*" ])
    |> Command.resp_from_command |> send_to_slave socket
  in
  Stdlib.print_endline (Resp.to_string result);
  Stdlib.print_endline "end of register_slave"

let notify_slaves (command : Resp.t) : unit =
  Stdlib.print_endline "start of notify_slaves";
  protect (fun () ->
      List.iter
        (fun socket ->
          let result = Resp.to_string command in
          Stdlib.print_endline "writing";
          Stdlib.print_endline result;
          ignore
            (Unix.write socket (Bytes.of_string result) 0 (String.length result)))
        !state);
  Stdlib.print_endline "end of of notify_slaves"
