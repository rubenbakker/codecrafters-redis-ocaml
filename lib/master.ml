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

let register_slave (socket : Unix.file_descr) : unit =
  protect (fun () ->
      state := socket :: !state;
      let result =
        ("REPLCONF", [ "GETACK"; "*" ])
        |> Command.resp_from_command |> Resp.to_string
      in
      ignore
        (Unix.write socket (Bytes.of_string result) 0 (String.length result));

      let buf = Bytes.create 2024 in
      let _bytes_read = Unix.read socket buf 0 2024 in
      Stdlib.print_endline (Bytes.to_string buf))

let notify_slaves (command : Resp.t) : unit =
  protect (fun () ->
      List.iter
        (fun socket ->
          let result = Resp.to_string command in
          ignore
            (Unix.write socket (Bytes.of_string result) 0 (String.length result)))
        !state)
