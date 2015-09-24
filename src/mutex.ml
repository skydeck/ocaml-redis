module Make(IO : Make.IO)(Client : module type of Client.Make(IO)) = struct

  exception Error of string

  let debug = ref false

  let (>>=) = IO.(>>=)

  type with_connection = {
    with_connection: 'a. (Client.connection -> 'a IO.t) -> 'a IO.t;
  }

  type lock_state = {
    lock_name: string;
    owner_id: string;
    mutable is_locked: bool;
      (* needs to be set to false just before releasing the lock *)
  }

  let make_tmp_lock_name lock_name owner_id =
    Printf.sprintf "tmp_%s_%s" owner_id lock_name

  let extend_in = 5 (* seconds *)
  let max_extend_by = extend_in + 2

  let expire conn lock_state time_remaining =
    let { lock_name; owner_id } = lock_state in
    if !debug then
      Printf.printf "[%.3f] expire lock %s in %i seconds, owner %s\n%!"
        (Unix.gettimeofday ()) lock_name time_remaining owner_id;
    Client.expire conn lock_name time_remaining >>= function
    | true ->
        IO.return ()
    | false ->
        IO.fail (
          Error ("Could not extend expiration of lock " ^ lock_name)
        )

  (*
     Extend expiration date incrementally in the background
     such that if the process dies, the lock doesn't stay around
     for too long.
  *)
  let rec extend_expiration_date with_connection lock_state time_remaining =
    if time_remaining > 0 && lock_state.is_locked then (
      (* Extend expiration date by at most 7 seconds,
         come back in 5 seconds to extend again if needed. *)
      let extend_by = min time_remaining max_extend_by in
      let time_remaining_then = time_remaining - extend_in in
      with_connection.with_connection (fun conn ->
        expire conn lock_state extend_by
      );
      if time_remaining_then > 0 then
        IO.sleep (float extend_in) >>= fun () ->
        extend_expiration_date with_connection lock_state time_remaining_then
      else
        IO.return ()
    )
    else
      IO.return ()

  let delete conn lock_name =
    if !debug then
      Printf.printf "[%.3f] delete lock %s\n%!"
        (Unix.gettimeofday ()) lock_name;
    Client.del conn [lock_name] >>= fun i ->
    IO.return ()

  (*
     Try to acquire a lock once, returning true if successful.
     Propagate exceptions if redis is not functioning as it should.
     The lock is guaranteed to expire.
  *)
  let try_acquire ltime with_connection lock_name owner_id =
    if !debug then
      Printf.printf "[%.3f] try acquire %s by %s\n%!"
        (Unix.gettimeofday ()) lock_name owner_id;

    let initial_lock_time =
      if not IO.asynchronous then ltime
      else min ltime max_extend_by
    in
    with_connection.with_connection (fun conn ->
      let tmp_lock_name = make_tmp_lock_name lock_name owner_id in
      if !debug then
        Printf.printf "[%.3f] \
          create temporary lock %s, expires in %i seconds\n%!"
          (Unix.gettimeofday ())
          tmp_lock_name initial_lock_time;
      Client.setex conn tmp_lock_name ltime owner_id >>= fun () ->
      (* if a crash occurs here, only the temporary lock remains,
         which doesn't block anyone *)
      IO.catch
        (fun () ->
          (* rename lock to the desired name,
             failing if it already exists *)
           if !debug then
             Printf.printf "[%.3f] try rename lock %s -> %s\n%!"
               (Unix.gettimeofday ()) tmp_lock_name lock_name;
           Client.renamenx conn tmp_lock_name lock_name
        )
        (fun e ->
           delete conn tmp_lock_name >>= fun () ->
           IO.fail e
        )
      >>= fun acquired ->
      if not acquired then
        delete conn tmp_lock_name >>= fun () ->
        IO.return None
      else (
        if !debug then
          Printf.printf "[%.3f] acquired %s by %s\n%!"
            (Unix.gettimeofday ()) lock_name owner_id;
        let lock_state = {
          lock_name;
          owner_id;
          is_locked = true;
        } in
        let time_remaining_then = ltime - extend_in in
        if IO.asynchronous then (
          if time_remaining_then > 0 then (
            IO.async (fun () ->
              IO.sleep (float extend_in) >>= fun () ->
              extend_expiration_date
                with_connection lock_state time_remaining_then
            )
          )
        )
        else
          assert (time_remaining_then = 0);
        IO.return (Some lock_state)
      )
    )

  let acquire ?(atime=10.) ?(ltime=10) with_connection lock_name owner_id =
    if not (ltime > 0) then
      invalid_arg "Redis.Mutex.acquire: ltime";
    if not (atime >= 0.) then
      invalid_arg "Redis.Mutex.acquire: atime";

    let etime = Unix.gettimeofday () +. atime in

    let rec loop sleep_amount =
      try_acquire ltime with_connection lock_name owner_id >>= function
      | Some lock_state ->
          IO.return lock_state
      | None ->
          if Unix.gettimeofday () < etime then
            IO.sleep (min sleep_amount 5. +. Random.float 0.1) >>= fun () ->
            loop (2. *. sleep_amount)
          else
            IO.fail (Error ("Could not acquire lock " ^ lock_name))
    in
    loop 0.2

  let release conn lock_state =
    (* Signal background job to not extend the lock's expiration time *)
    lock_state.is_locked <- false;
    let {lock_name; owner_id} = lock_state in
    Client.watch conn [lock_name] >>= fun _ ->
    Client.get conn lock_name >>= function
    | Some x when x = owner_id ->
        Client.multi conn >>= fun _ ->
        Client.queue (fun () -> delete conn lock_name) >>= fun () ->
        Client.exec conn >>= fun _ ->
        IO.return ()
    | _ ->
        Client.unwatch conn >>= fun _ ->
        IO.fail (Error ("Lock was lost: " ^ lock_name))

  let with_mutex ?atime ?ltime with_connection lock_name fn =
    let owner_id = Uuidm.(to_string (create `V4)) in
    acquire ?atime ?ltime with_connection lock_name owner_id
    >>= fun lock_state ->
    IO.catch
      (fun () ->
         fn () >>= fun res ->
         with_connection.with_connection
           (fun conn -> release conn lock_state) >>= fun () ->
         IO.return res)
      (fun e ->
         if !debug then
           Printf.printf "[%.3f] exception with lock %s: %s\n%!"
             (Unix.gettimeofday ()) lock_name (Printexc.to_string e);
         with_connection.with_connection
           (fun conn -> release conn lock_state) >>= fun () ->
         IO.fail e)
end
