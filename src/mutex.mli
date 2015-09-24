module Make(IO : Make.IO)(Client : module type of Client.Make(IO)) : sig

  val debug : bool ref

  exception Error of string

  type with_connection = {
    with_connection: 'a. (Client.connection -> 'a IO.t) -> 'a IO.t;
  }
    (** The [with_connection] field is a function that provides
        to a given function exclusive access to a redis connection and
        takes care of recycling or closing the connection
        when done.

        The goal is to not hold on to the connection for too long.

        The reason why [with_connection] needs to be wrapped in a record
        is for preserving its polymorphism so it can be used
        in the same function body with two different types as argument.
    *)

  type lock_state

  val acquire :
    ?atime:float ->
    ?ltime:int ->
    with_connection ->
    string -> string -> lock_state IO.t
    (** [acquire with_connection mutex_name unique_name]
        tries to acquire a lock exclusively, retrying with exponential
        backoff for [atime] seconds.
        Once acquired, the lock expires after [ltime] seconds
        unless released earlier.
    *)

  val release : Client.connection -> lock_state -> unit IO.t
    (** [release conn lock_state mutex mutex_name unique_name] releases
        the mutex if possible. *)

  val with_mutex :
    ?atime:float ->
    ?ltime:int ->
    with_connection -> string -> (unit -> 'a IO.t) -> 'a IO.t
    (** [with_mutex with_connection mutex_name f] runs
        function [f] exclusively while holding the lock [mutex_name],
        unless it takes too long and the lock expires.
        See [acquire] for the meaning of [atime] and [ltime]. *)
end
