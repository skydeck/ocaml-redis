module IO : Make.IO

module Client : module type of Client.Make(IO)
module Cache : module type of Cache.Make(IO)(Client)
module Mutex : module type of Mutex.Make(IO)(Client)
