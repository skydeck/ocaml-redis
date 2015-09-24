open Redis

module IO = struct
  type 'a t = 'a Lwt.t
  type file_descr = Lwt_unix.file_descr
  type in_channel = Lwt_chan.in_channel
  type out_channel = Lwt_chan.out_channel

  let asynchronous = true

  let (>>=) = Lwt.(>>=)
  let catch = Lwt.catch
  let try_bind = Lwt.try_bind
  let ignore_result = Lwt.ignore_result
  let return = Lwt.return
  let fail = Lwt.fail
  let async = Lwt.async

  let socket = Lwt_unix.socket
  let connect = Lwt_unix.connect
  let close = Lwt_unix.close
  let sleep = Lwt_unix.sleep

  let in_channel_of_descr = Lwt_chan.in_channel_of_descr
  let out_channel_of_descr = Lwt_chan.out_channel_of_descr
  let input_char = Lwt_chan.input_char
  let really_input = Lwt_chan.really_input
  let output_string = Lwt_chan.output_string
  let flush = Lwt_chan.flush

  let iter = Lwt_util.iter
  let iter_serial = Lwt_util.iter_serial
  let map = Lwt_util.map
  let map_serial = Lwt_util.map_serial
  let fold_left = Lwt_util.fold_left
end

module Client = Client.Make(IO) with 
module Cache = Cache.Make(IO)(Client)
module Mutex = Mutex.Make(IO)(Client)

(*
   Testing

   For now, this how to run the tests:

   ./configure --enable-lwt
   make
   make reinstall
   utop
   #use "topfind";;
   #require "redis.lwt";;
   Redis_lwt.test ();;

 *)

open Printf
open Lwt

let time name f =
  let t1 = Unix.gettimeofday () in
  f () >>= fun result ->
  let t2 = Unix.gettimeofday () in
  let dt = t2 -. t1 in
  printf "%s: %.3f seconds\n%!" name dt;
  return (result, dt)

let print_time name f =
  time name f >>= fun (x, t) ->
  return x

let with_connection f =
  Client.connect {
    Client.host = "127.0.0.1";
    port = 6379
  } >>= fun conn ->
  finalize
    (fun () -> f conn)
    (fun () -> Client.disconnect conn)

let wc = { Mutex.with_connection }

let test_sequence () =
  let name = "test_sequence" in
  let job () =
    Mutex.with_mutex ~atime:1. ~ltime:1 wc name (fun () ->
      Lwt_unix.sleep 0.010
    )
  in
  time "all" (fun () ->
    print_time "job1" job >>= fun () ->
    print_time "job2" job
  ) >>= fun ((), t) ->
  return (t <= 0.03)

let test_exception () =
  let name = "test_exception" in
  let wm f = fun () ->
    Mutex.with_mutex ~atime:1. ~ltime:1 wc name f
  in
  time "all" (fun () ->
    catch
      (fun () ->
         print_time "job1" (wm (fun () -> failwith "test"))
      )
      (fun e -> return ())
    >>= fun () ->
    print_time "job2" (wm (fun () -> Lwt_unix.sleep 0.010))
  ) >>= fun ((), t) ->
  return (t <= 0.03)

let test_short () =
  let name = "test_short" in
  let job () =
    Mutex.with_mutex ~atime:1. ~ltime:1 wc name (fun () ->
      Lwt_unix.sleep 0.010
    )
  in
  let job1 = print_time "job1" job in
  let job2 = print_time "job2" job in
  time "all" (fun () ->
    join [job1; job2]
  ) >>= fun ((), t) ->
  return (t >= 0.2 && t <= 0.4)

let test_long () =
  let job_duration = 5. in
  let max_acquisition_time = 5. in
  let name = "test_long" in
  let job () =
    Mutex.with_mutex ~atime:30. ~ltime:30 wc name (fun () ->
      Lwt_unix.sleep job_duration
    )
  in
  let job1 = print_time "job1" job in
  let job2 = print_time "job2" job in
  let job3 = print_time "job3" job in
  time "all" (fun () ->
    join [job1; job2; job3]
  ) >>= fun ((), t) ->
  return (
    t >= 3. *. job_duration
    && t <= 3. *. (job_duration +. 2. *. max_acquisition_time)
  )

let test_expire () =
  catch
    (fun () ->
       Mutex.with_mutex ~atime:0.1 ~ltime:11 wc "test_expire" (fun () ->
         Lwt_unix.sleep 12.
       ) >>= fun () ->
       return false
    )
    (function
      | (Mutex.Error _ as e) ->
          printf "Expected lock timeout: %s\n%!" (Printexc.to_string e);
          return true
      | e ->
          raise e
    )

let tests = [
  "sequence", test_sequence;
  "exception", test_exception;
  "short", test_short;
  "long", test_long;
  "expire", test_expire;
]

let run_tests test_list =
  let results =
    List.map (fun (name, f) ->
      let job =
        catch
          (fun () ->
             printf "TEST %s\n%!" name;
             f () >>= fun success ->
             return (name, success)
          )
          (fun e ->
             printf "Exception: %s\n%!" (Printexc.to_string e);
             return (name, false)
          )
      in
      Lwt_main.run job
    ) test_list
  in
  List.iter (fun (name, success) ->
    printf "%-15s%s\n" name (if success then "OK" else "FAILED")
  ) results

let test () =
  Mutex.debug := true;
  run_tests tests;
  Mutex.debug := false
