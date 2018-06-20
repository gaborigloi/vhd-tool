module F = Vhd_format.F.From_file(Vhd_format_lwt.IO)

open Lwt.Infix

let cleanup_on_exn f cleanup = Lwt.catch f (fun e -> cleanup () >>= fun () -> Lwt.fail e)

let min a b = if Int64.compare a b < 0 then a else b
let assert_integer_sectors b =
  if Int64.rem b 512L <> 0L then failwith "Expecting sector aligned extents"
let to_sectors b = Int64.div b 512L
let to_bytes s =
  let res = Int64.mul s 512L in
  if res <= s then
    failwith (Printf.sprintf "Overflow while converting sectors to bytes: %Ld sectors -> %Ld bytes" s res)
  else res
let to_int i =
  let res = Int64.to_int i in
  if (Int64.of_int res) <> i then
    failwith (Printf.sprintf "int64 %Ld exceeds maximum int: converted to int %d" i res)
  else res

type extent = {
  flags : int32;
  length : int64;
} [@@deriving rpc]

(* The flags returned for the base:allocation NBD metadata context are defined here:
   https://github.com/NetworkBlockDevice/nbd/blob/extension-blockstatus/doc/proto.md#baseallocation-metadata-context *)
let flag_hole = 1l
let flag_zero = 2l
type extent_list = extent list [@@deriving rpc]

(** We query the block status for an area of up to 1GiB at a time, to avoid
    excessive memory usage when marshalling/unmarshalling the JSON containing
    the extent list. *)
let max_query_length = Int64.(mul 1024L (mul 1024L 1024L))

(** In case the source is an NBD server, we donwload at most 1MiB at a time *)
let max_data_chunk_length = Int64.(mul 1024L 1024L)
let max_data_chunk_length_sectors = to_sectors max_data_chunk_length

(** The extents returned by this Python script must be consecutive,
    non-overlapping, in the correct order starting from the specified offset,
    and must exactly cover the requested area. *)
let get_extents_json ~extent_reader ~server ~export_name ~offset ~length =
  Lwt_process.pread
    ("", [|extent_reader; "--path"; server; "--exportname"; export_name; "--offset"; Int64.to_string offset; "--length"; Int64.to_string length|])

(** Returns a stream of classified extents by repeatedly calling the
    get_nbd_extents script to query the block status from the NBD server for
    the next portion of the disk *)
let get_extents ?(extent_reader="/opt/xensource/libexec/get_nbd_extents.py") server export_name size =
  let is_empty e =
    let has_flag flag =
      Int32.logand e.flags flag = flag
    in
    (* We assume the destination is prezeroed, so we do not have to copy zeroed extents *)
    (has_flag flag_hole) || (has_flag flag_zero)
  in
  let rec operations extents offset acc =
    match extents with
    | e::es ->
      assert_integer_sectors e.length;
      let op =
        if is_empty e
        then `Empty (to_sectors e.length)
        else `Data (to_sectors offset, to_sectors e.length)
      in
      operations es (Int64.add offset e.length) (op::acc)
    | [] -> (List.rev acc, offset)
  in
  let operations ~offset ~length =
    get_extents_json ~extent_reader ~server ~export_name ~offset ~length >>= fun extents_json ->
    let extents = extent_list_of_rpc (Jsonrpc.of_string extents_json) in
    let ops, final_offset = operations extents offset [] in
    (if final_offset <> (Int64.add offset length)
     then Lwt.fail_with (Printf.sprintf "Nbd_input.get_extents: extents returned for offset=%Ld & length=%Ld finished at incorrect offset %Ld," offset length final_offset)
     else Lwt.return_unit) >|= fun () ->
    ops
  in

  let rec block ops offset =
    match ops, offset with
    | [], offset when offset >= size ->
      (if offset <> size then Lwt.fail_with (Printf.sprintf "Nbd_input.get_extents finished with offset=%Ld <> size=%Ld" offset size)
       else Lwt.return_unit) >>= fun () ->
      Lwt.return F.End
    | [], _ ->
      let length = min (Int64.sub size offset) max_query_length in
      operations ~offset ~length >>= fun ops ->
      block ops (Int64.add offset length)
    | op::ops, _ -> Lwt.return (F.Cons (op, fun () -> block ops offset))
  in
  block [] 0L

(** Converts the given stream to a new one using a function that returns a
    stream of multiple elements for each element. The user can specify some
    cleanup actions to run when the end of the stream is reached. These
    cleanups will be performed by the resulting stream before returning
    [F.End]. *)
let flat_map_stream ~close f s =
  let rec loop = function
    | F.End, F.Cons (x, t) ->
      f x >>= fun xs ->
      t () >>= fun t ->
      loop (xs, t)
    | F.Cons (b, t), s ->
      F.Cons (b, fun () -> t () >>= fun t -> loop (t, s))
      |> Lwt.return
    | F.End, F.End -> close () >|= fun () -> F.End
  in
  loop (F.End, s)

let rec map_stream f = function
  | F.Cons (a, t) ->
    F.Cons (f a, fun () -> t () >|= map_stream f)
  | F.End -> F.End

let hybrid raw server export_name size =
  get_extents server export_name size >>= fun extents ->
  let elements =
    map_stream
      (function
        | `Data (offset, length) -> `Copy (raw, offset, length)
        | `Empty _ as e -> e
      )
      extents
  in
  let size = Vhd_format.F.{ total = size; metadata = 0L; empty = 0L; copy = 0L } in
  Lwt.return F.{elements; size}

exception NbdError of Nbd_lwt_unix.Client.error

let nbd server export_name =
  (** Returns the data in the given extent in chunks as a data stream. *)
  let rec read t ~sector_offset ~end_sectors =
    if sector_offset >= end_sectors then begin
      (if sector_offset <> end_sectors
       then Lwt.fail_with (Printf.sprintf "Nbd_input.nbd finished reading with sector_offset=%Ld <> end_sectors=%Ld" sector_offset end_sectors)
       else Lwt.return_unit) >|= fun () ->
      F.End
    end else begin
      let data_length_sectors =
        min max_data_chunk_length_sectors (Int64.sub end_sectors sector_offset)
      in
      let data_length = to_bytes data_length_sectors in
      let buf = Cstruct.create_unsafe (to_int data_length) in
      Nbd_lwt_unix.Client.read t data_length [buf] >>= function
      | Ok () ->
        let next_offset = Int64.add sector_offset (to_sectors data_length) in
        F.Cons (`Sectors buf, fun () -> read t ~sector_offset:next_offset ~end_sectors)
        |> Lwt.return
      | Error e -> Lwt.fail (NbdError e)
    end
  in

  let s = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
  cleanup_on_exn
    (fun () -> Lwt_unix.connect s (Lwt_unix.ADDR_UNIX server))
    (fun () -> Lwt_unix.close s)
  >>= fun () ->
  let c = Nbd_lwt_unix.cleartext_channel_of_fd s None in
  let c = Nbd.Channel.generic_of_cleartext_channel c in
  cleanup_on_exn
    (fun () -> Nbd_lwt_unix.Client.negotiate c export_name)
    (* This will close the socket too *)
    c.Nbd.Channel.close
  >>= fun (t, size, _) ->

  let close () =
    (* This currently won't close the channel *)
    Nbd_lwt_unix.Client.disconnect t >>= fun () ->
    (* This will close the socket too *)
    c.Nbd.Channel.close ()
  in

  cleanup_on_exn
    (fun () ->
       get_extents server export_name size >>= fun extents ->
       (* We keep reading the allocated data from the NBD server. When the
          stream is consumed by the user and we reach the end, we perform the
          necessary cleanups to close the open channels. *)
       flat_map_stream
         ~close
         (function
           | `Data (sector_offset, length_sectors) ->
             let end_sectors = Int64.add sector_offset length_sectors in
             read t ~sector_offset ~end_sectors
           | `Empty _ as e ->
             F.Cons (e, fun () -> Lwt.return F.End) |> Lwt.return
         )
         extents
       >>= fun elements ->
       let size = Vhd_format.F.{ total = size; metadata = 0L; empty = 0L; copy = 0L } in
       Lwt.return F.{elements; size}
    )
    close
