(* error response *)
exception Error of string

module Make(IO : Make.IO)(Client : module type of Client.Make(IO)) = struct
  open Client

  let (>>=) = IO.(>>=)

  let acquire conn ?(atime=10.) ?(ltime=10) mutex id =
    let etime = Unix.gettimeofday () +. atime in

    let update_ttl () =
      ttl conn mutex >>= function
        | None -> expire conn mutex ltime >>= fun _ -> IO.return ()
        | _ -> IO.return () in

    let rec loop sleep_amount =
      setnx conn mutex id >>= function
      | true ->
          expire conn mutex ltime >>= fun _ ->
          IO.return ()
      | _ ->
          update_ttl () >>= fun _ ->
          if Unix.gettimeofday () < etime then
            IO.sleep (sleep_amount +. Random.float 0.5) >>= fun () ->
            loop (2. *. sleep_amount)
          else
            IO.fail (Error ("could not acquire lock " ^ mutex))
    in
    loop 0.2

  let release conn mutex id =
    watch conn [mutex] >>= fun _ -> get conn mutex >>= function
      | Some x when x = id ->
          multi conn >>= fun _ ->
          queue (fun () -> del conn [mutex]) >>= fun _ ->
          exec conn >>= fun _ ->
          IO.return ()
      | _ ->
          unwatch conn >>= fun _ ->
          IO.fail (Error ("lock was lost: " ^ mutex))

  let with_mutex conn ?atime ?ltime mutex fn =
    let id = Uuidm.(to_string (create `V4)) in
    acquire conn ?atime ?ltime mutex id >>= fun _ ->
    IO.catch
    (* try *) (fun () ->
      fn () >>= fun res ->
      release conn mutex id >>= fun _ ->
      IO.return res)
    (* catch *) (function e ->
      release conn mutex id >>= fun _ ->
      IO.fail e)
end
