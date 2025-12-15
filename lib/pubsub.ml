open Base

type subscriber_t = { thread_id : int }
type channel_t = { name : string; mutable subscribers : subscriber_t list }

let channels : channel_t list ref = ref []

let find_channel (channel_name : string) : channel_t option =
  List.find ~f:(fun channel -> String.(channel.name = channel_name)) !channels

let subscribe (channel_name : string) (thread_id : int) : unit =
  match find_channel channel_name with
  | Some channel -> channel.subscribers <- { thread_id } :: channel.subscribers
  | None ->
      channels :=
        { name = channel_name; subscribers = [ { thread_id } ] } :: !channels

let publish (channel_name : string) : int =
  match find_channel channel_name with
  | Some channel -> List.length channel.subscribers
  | None -> 0

let channels_for_subscriber (thread_id : int) : string list =
  List.fold !channels ~init:[] ~f:(fun acc channel ->
      match
        List.find ~f:(fun s -> s.thread_id = thread_id) channel.subscribers
      with
      | Some _ -> channel.name :: acc
      | None -> acc)
