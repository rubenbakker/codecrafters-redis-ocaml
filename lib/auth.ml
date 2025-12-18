open! Base

type user_t = { username : string; password : string option }
type t = user_t list

module Users = Protected.Resource.Make (struct
  type t = (string, user_t) Hashtbl.t

  let state = Hashtbl.create (module String)
  let get () = state
  let set _u = ()

  let () =
    Hashtbl.set state ~key:"default"
      ~data:{ username = "default"; password = None }
end)

let get_user (username : string) : Resp.t =
  Users.query (fun users ->
      match Hashtbl.find users username with
      | Some user ->
          let password_string =
            match user.password with None -> "nopass" | Some hash -> hash
          in
          Resp.RespList
            [
              Resp.BulkString "flags";
              Resp.RespList [ Resp.BulkString password_string ];
              Resp.BulkString "passwords";
              Resp.RespList [];
            ]
      | None -> Resp.Null)

let set_password (username : string) (password : string) : Resp.t =
  Users.mutate_with_result (fun users ->
      match Hashtbl.find users username with
      | Some user ->
          let password_hash =
            Digestif.SHA256.digest_string password |> Digestif.SHA256.to_hex
          in
          Hashtbl.set users ~key:username
            ~data:{ user with password = Some password_hash };
          (users, Resp.BulkString "OK")
      | None -> (users, Resp.Null))
