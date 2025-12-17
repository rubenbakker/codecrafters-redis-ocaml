module Resource = struct
  module type Unprotected = sig
    type t

    val get : unit -> t
    val set : t -> unit
  end

  module type Protected = sig
    type t

    val get : unit -> t
    val query : (t -> 'a) -> 'a
    val mutate : (t -> t) -> unit
  end

  module Make (R : Unprotected) : Protected with type t := R.t = struct
    let lock = Stdlib.Mutex.create ()
    let unlock () : unit = Stdlib.Mutex.unlock lock
    let lock () : unit = Stdlib.Mutex.lock lock

    let query (fn : R.t -> 'a) =
      Stdlib.Fun.protect ~finally:unlock (fun () ->
          lock ();
          fn (R.get ()))

    let mutate (fn : R.t -> R.t) =
      Stdlib.Fun.protect ~finally:unlock (fun () ->
          lock ();
          fn (R.get ()) |> R.set)

    let get = R.get
  end
end
