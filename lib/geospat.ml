open Base

let min_latitude = -85.05112878
let max_latitude = 85.05112878
let min_longitude = -180.0
let max_longitude = 180.0
let latitude_range = max_latitude -. min_latitude
let longitude_range = max_longitude -. min_longitude

let longitute_of_string (longitude : string) : (float, string) Result.t =
  match Float.of_string_opt longitude with
  | Some longitude ->
      if Float.(longitude >= -180.0 && longitude <= 180.0) then Ok longitude
      else Error (Printf.sprintf "ERR longitude (%f) is invalid" longitude)
  | None -> Error "ERR invalid longitude value"

let latitude_of_string (latitude : string) : (float, string) Result.t =
  match Float.of_string_opt latitude with
  | Some latitude ->
      if Float.(latitude >= -85.05112878 && latitude <= 85.05112878) then
        Ok latitude
      else Error (Printf.sprintf "ERR latitude (%f) is invalid" latitude)
  | None -> Error "ERR invalid latitude value"

let geocode_of_strings (longitude : string) (latitude : string) :
    (float * float, string) Result.t =
  match longitute_of_string longitude with
  | Ok longitude -> (
      match latitude_of_string latitude with
      | Ok latitude -> Ok (longitude, latitude)
      | Error _ as m -> m)
  | Error _ as e -> e

module Encode = struct
  let spread_int32_to_int64 v =
    let open Stdlib in
    let v = Int64.logand v 0xFFFFFFFFL in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_left v 16)) 0x0000FFFF0000FFFFL
    in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_left v 8)) 0x00FF00FF00FF00FFL
    in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_left v 4)) 0x0F0F0F0F0F0F0F0FL
    in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_left v 2)) 0x3333333333333333L
    in
    Int64.logand (Int64.logor v (Int64.shift_left v 1)) 0x5555555555555555L

  let interleave x y =
    let open Stdlib in
    let x_spread = spread_int32_to_int64 (Int64.of_int32 x) in
    let y_spread = spread_int32_to_int64 (Int64.of_int32 y) in
    let y_shifted = Int64.shift_left y_spread 1 in
    Int64.logor x_spread y_shifted

  let encode latitude longitude =
    let open Stdlib in
    (* Normalize to the range 0-2^26 *)
    let normalized_latitude =
      (2.0 ** 26.0) *. (latitude -. min_latitude) /. latitude_range
    in
    let normalized_longitude =
      (2.0 ** 26.0) *. (longitude -. min_longitude) /. longitude_range
    in

    (* Truncate to integers *)
    let lat_int = Int32.of_float normalized_latitude in
    let lon_int = Int32.of_float normalized_longitude in

    interleave lat_int lon_int
end

module Decode = struct
  type coordinates = { latitude : float; longitude : float }

  let compact_int64_to_int32 v =
    let open Stdlib in
    let v = Int64.logand v 0x5555555555555555L in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_right v 1)) 0x3333333333333333L
    in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_right v 2)) 0x0F0F0F0F0F0F0F0FL
    in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_right v 4)) 0x00FF00FF00FF00FFL
    in
    let v =
      Int64.logand (Int64.logor v (Int64.shift_right v 8)) 0x0000FFFF0000FFFFL
    in
    Int64.to_int32
      (Int64.logand
         (Int64.logor v (Int64.shift_right v 16))
         0x00000000FFFFFFFFL)

  let convert_grid_numbers_to_coordinates grid_latitude_number
      grid_longitude_number =
    (* Calculate the grid boundaries *)
    let open Stdlib in
    let grid_latitude_min =
      min_latitude
      +. latitude_range
         *. (Int32.to_float grid_latitude_number /. (2.0 ** 26.0))
    in
    let grid_latitude_max =
      min_latitude
      +. latitude_range
         *. (Int32.to_float (Int32.add grid_latitude_number 1l) /. (2.0 ** 26.0))
    in
    let grid_longitude_min =
      min_longitude
      +. longitude_range
         *. (Int32.to_float grid_longitude_number /. (2.0 ** 26.0))
    in
    let grid_longitude_max =
      min_longitude
      +. longitude_range
         *. (Int32.to_float (Int32.add grid_longitude_number 1l)
            /. (2.0 ** 26.0))
    in

    (* Calculate the center point of the grid cell *)
    let latitude = (grid_latitude_min +. grid_latitude_max) /. 2.0 in
    let longitude = (grid_longitude_min +. grid_longitude_max) /. 2.0 in

    { latitude; longitude }

  let decode geo_code =
    (* Align bits of both latitude and longitude to take even-numbered position *)
    let y = Int64.shift_right geo_code 1 in
    let x = geo_code in

    (* Compact bits back to 32-bit ints *)
    let grid_latitude_number = compact_int64_to_int32 x in
    let grid_longitude_number = compact_int64_to_int32 y in

    convert_grid_numbers_to_coordinates grid_latitude_number
      grid_longitude_number
end
