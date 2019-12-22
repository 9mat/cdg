set more off

if "`c(hostname)'" == "powercat" {
  global cdgpath "E:/cdg_data"
  global commonpath "D:/Dropbox/work/data/dta"
  global imppath "F:/Temp"
  global ivupath "F:/CDGData/vehicle_location"

  global vehiclematchfile $cdgpath/generated/vehicle_cd_match.dta
  global drivermatchfile $cdgpath/generated/driver_cd_match.dta

  global cnsfile "F:/Temp/cns.dta"

  global tmp "F:/Temp"
}

if "`c(hostname)'" == "musang01" | strpos("`c(hostname)'", "comp") > 0 | strpos("`c(hostname)'", "hpc") > 0 {
  global project01 "/NAS/project01/chujunhong_SPIRE"

  global cdgpath "$project01/cdg_data"
  global commonpath "$project01/common"
  global ivupath "$project01/vehicle_location"
  global imppath "$project01/cdg2/impute"

  global vehiclematchfile $project01/common/vehicle_cd_match.dta
  global drivermatchfile $project01/common/driver_cd_match.dta

  global cnsfile "$project01/cdg2/cns.dta"

  global trip201912 "$project01/trips/tmp191122.dta"
  global sterdir "/NAS/home01/chujunhong/cdg/ster"

  global tmp "/scratch"
}

if "`c(hostname)'" == "cybermate" {
  global cdgpath "C:/data"
  global commonpath "C:/Users/long/Dropbox/work/data/dta"
  global ivupath "C:/data"

  global tmp "C:/data/tmp"


  global vehiclematchfile C:/data/vehicle_cd_match.dta
  global drivermatchfile C:/data/driver_cd_match.dta

}
