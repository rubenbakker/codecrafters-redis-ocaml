type slave_t = { socket : Unix.file_descr; bytes_sent : int; bytes_ack : int }
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

let state : slave_t list ref = ref []

let register_slave (socket : Unix.file_descr) (rdb : Resp.t option) : unit =
  protect (fun () ->
      state := { socket; bytes_sent = 0; bytes_ack = 0 } :: !state);
  match rdb with
  | Some rdb ->
      let rdb_string = Resp.to_string rdb in
      ignore
        (Unix.write socket
           (Bytes.of_string rdb_string)
           0 (String.length rdb_string))
  | None -> ()

let notify_slaves (command : Resp.t) : unit =
  protect (fun () ->
      let new_list : slave_t list ref = ref [] in
      List.iter
        (fun slave ->
          let result = Resp.to_string command in
          let payload_length = String.length result in
          ignore
            (Unix.write slave.socket (Bytes.of_string result) 0 payload_length);
          new_list :=
            { slave with bytes_sent = slave.bytes_sent + payload_length }
            :: !new_list)
        !state;
      state := !new_list)

let num_slaves () : int = protect (fun () -> List.length !state)
