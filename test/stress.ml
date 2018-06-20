module F = Vhd_format.F.From_file(Vhd_format_lwt.IO)

open Lwt.Infix

(** We simulate a 4TiB disk *)
let size = Int64.(mul 4L (mul 1024L (mul 1024L (mul 1024L 1024L))))

let rec process_stream total = function
  | F.Cons (data, s) ->
    let sectors = match data with
      | `Empty sectors -> sectors
      | `Data (offset, sectors) ->
        if offset <> total then begin
          Alcotest.failf "wrong offset: processed %Ld, offset is %Ld" total offset;
        end;
        sectors
      | _ -> failwith "unexpected element"
    in
    s () >>= fun s ->
    process_stream (Int64.add total sectors) s
  | F.End -> Lwt.return total

let test_huge_input switch () =
  let server = "" in
  let export_name = "" in
  Nbd_input.get_extents ~extent_reader:"./dummy_extent_reader.py" server export_name size >>= fun extents ->
  process_stream 0L extents >|= fun sectors ->
  Alcotest.(check int64) "total size of elements in stream"
    size
    (Int64.mul 512L sectors)

let test_set =
  let t = Alcotest_lwt.test_case in
  [ t "VDI with a large allocated extent list" `Quick test_huge_input ]

let () =
  Alcotest.run "stress test"
    [ "Nbd_input", test_set ]
