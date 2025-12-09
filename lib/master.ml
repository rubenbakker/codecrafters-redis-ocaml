open! Base

type slave_t = {
  socket : Unix.file_descr;
  mutable bytes_sent : int;
  mutable bytes_ack : int;
}

type wait_listener_t = {
  lock : Stdlib.Mutex.t;
  condition : Stdlib.Condition.t;
  num_required_slaves : int;
  lifetime : Lifetime.t;
  mutable num_ack_slaves : int;
  mutable result : int option;
}

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
let wait_listeners : wait_listener_t list ref = ref []

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
      List.iter
        ~f:(fun slave ->
          let result = Resp.to_string command in
          let payload_length = String.length result in
          ignore
            (Unix.write slave.socket (Bytes.of_string result) 0 payload_length);
          slave.bytes_sent <- slave.bytes_sent + payload_length;
          ())
        !state)

let num_insync_slaves () : int =
  protect (fun () ->
      !state
      |> List.filter ~f:(fun slave -> slave.bytes_sent = slave.bytes_ack)
      |> List.length)

let process_replconf_ack (slave : slave_t) =
  let command =
    Resp.RespList
      [
        Resp.BulkString "REPLCONF";
        Resp.BulkString "GETACK";
        Resp.BulkString "*";
      ]
  in
  Stdlib.print_endline (Sexp.to_string (Resp.to_sexp command));
  let command_payload = Resp.to_string command in
  let outch = Unix.out_channel_of_descr slave.socket in
  Stdlib.Printf.fprintf outch "%s" command_payload;
  Stdlib.flush outch;
  let inch = Unix.in_channel_of_descr slave.socket in
  Stdlib.print_endline "reading from in channel";
  try
    match Resp.read_from_channel inch with
    | Resp.RespList l, _ -> (
        Stdlib.print_endline "got result";
        Stdlib.print_endline (Sexp.to_string (Resp.to_sexp (RespList l)));
        match l with
        | [
         Resp.BulkString "REPLCONF";
         Resp.BulkString "ACK";
         Resp.BulkString bytes_ack;
        ] ->
            ignore
            @@ protect (fun () -> slave.bytes_ack <- Int.of_string bytes_ack);
            if true then (
              Stdlib.print_endline "same bytes!";
              List.iter
                ~f:(fun wl ->
                  wl.num_ack_slaves <- wl.num_ack_slaves + 1;
                  Stdlib.print_endline ">>> num_ack_slaves";
                  Stdlib.print_int wl.num_ack_slaves;
                  Stdlib.print_int wl.num_required_slaves;
                  Stdlib.print_endline "<<<";
                  if wl.num_ack_slaves >= wl.num_required_slaves then (
                    wl.result <- Some wl.num_ack_slaves;
                    ignore
                    @@ protect (fun () ->
                        wait_listeners :=
                          List.filter
                            ~f:(fun w -> Option.is_none w.result)
                            !wait_listeners);
                    Stdlib.Condition.signal wl.condition)
                  else ())
                !wait_listeners;
              ignore
              @@ protect (fun () ->
                  slave.bytes_sent <-
                    slave.bytes_sent + String.length command_payload))
            else (
              Stdlib.Printf.printf "Not the same %d != %d" slave.bytes_ack
                slave.bytes_sent;
              Stdlib.flush Stdlib.stdout)
        | _ -> Stdlib.print_endline "unknown resp - not an ack")
    | _ -> Stdlib.print_endline "unknown resp - not a list"
  with End_of_file ->
    Stdlib.print_endline "end of file received waiting for slave"

let start_sync_slaves () =
  List.iter
    ~f:(fun slave ->
      ignore @@ Thread.create (fun () -> process_replconf_ack slave) ())
    !state

let sync_slaves_for_listener (required_slaves : int) (lifetime : Lifetime.t) :
    int =
  let current_insync_slaves = num_insync_slaves () in
  if current_insync_slaves >= required_slaves then current_insync_slaves
  else
    let listener =
      {
        lock = Stdlib.Mutex.create ();
        condition = Stdlib.Condition.create ();
        num_required_slaves = required_slaves;
        num_ack_slaves = current_insync_slaves;
        lifetime;
        result = None;
      }
    in
    ignore (protect (fun () -> wait_listeners := listener :: !wait_listeners));
    start_sync_slaves ();
    Stdlib.Mutex.lock listener.lock;
    Stdlib.Condition.wait listener.condition listener.lock;
    listener.num_ack_slaves
