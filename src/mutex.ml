(* error response *)
exception Error of string

module Make(IO : Make.IO)(Client : module type of Client.Make(IO)) = struct
  let (>>=) = IO.(>>=)

  type with_connection = {
    with_connection: 'a. (Client.connection -> 'a IO.t) -> 'a IO.t;
  }

  let acquire ?(atime=10.) ?(ltime=10) {with_connection} mutex id =
    let etime = Unix.gettimeofday () +. atime in

    let rec loop sleep_amount =
      with_connection (fun conn ->
        Client.setnx conn mutex id >>= fun acquired ->
        if acquired then
          Client.expire conn mutex ltime >>= fun success ->
          if success then
            IO.return true
          else
            IO.fail (Error ("Could not set expiration for lock " ^ mutex))
        else
          IO.return false
      ) >>= fun success ->
      if success then
        IO.return ()
      else if Unix.gettimeofday () < etime then
        IO.sleep (min sleep_amount 5. +. Random.float 0.5) >>= fun () ->
        loop (2. *. sleep_amount)
      else
        IO.fail (Error ("Could not acquire lock " ^ mutex))
    in
    loop 0.2

  let release conn mutex id =
    Client.watch conn [mutex] >>= fun _ ->
    Client.get conn mutex >>= function
    | Some x when x = id ->
        Client.multi conn >>= fun _ ->
        Client.queue (fun () -> Client.del conn [mutex]) >>= fun _ ->
        Client.exec conn >>= fun _ ->
        IO.return ()
    | _ ->
        Client.unwatch conn >>= fun _ ->
        IO.fail (Error ("lock was lost: " ^ mutex))

  let with_mutex ?atime ?ltime with_connection mutex fn =
    let id = Uuidm.(to_string (create `V4)) in
    acquire ?atime ?ltime with_connection mutex id >>= fun _ ->
    IO.catch
      (fun () ->
         fn () >>= fun res ->
         with_connection.with_connection
           (fun conn -> release conn mutex id) >>= fun () ->
         IO.return res)
      (fun e ->
         with_connection.with_connection
           (fun conn -> release conn mutex id) >>= fun () ->
         IO.fail e)
end
