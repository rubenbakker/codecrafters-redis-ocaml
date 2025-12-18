open! Base

let get_user (_username : string) : Resp.t =
  Resp.RespList
    [ Resp.BulkString "flags"; Resp.RespList [ Resp.BulkString "nopass" ] ]
