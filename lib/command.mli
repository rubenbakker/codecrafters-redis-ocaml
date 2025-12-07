open Base
type command_t = string * string list
type command_queue_t = command_t Queue.t
type post_process_t = RegisterSlave | Mutation of Resp.t | Noop

type context_t = {
  role : Options.role_t;
  command_queue : command_queue_t option;
  post_process : post_process_t;
}


(** process commands and returns a Resp.t datastructure *)
val process : context_t -> string -> Resp.t * context_t 
