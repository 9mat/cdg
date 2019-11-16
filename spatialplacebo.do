set more off

include definepath.do

global sg_lat 1.28
global sg_lon 103.8

geodist $sg_lat `=$sg_lon-0.1' $sg_lat `=$sg_lon+0.1'
global kmperlon `=`r(distance)'/0.2'

geodist `=$sg_lat-0.1' $sg_lon `=$sg_lat+0.1' $sg_lon
global kmperlat `=`r(distance)'/0.2'

// bandwidth (or window size) for distance in metres and bandwidth for time in minutes
global bwx_m 1000
global bwy_m 1000
global bwt_mins 5





capture program drop prepare_master
program define prepare_master
  syntax [using/], [saveto(string)]

  if "`using'" != "" use "`using'", clear  

  keep if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)
  keep id ref_postcode ref_end_dt driver_cd
  merge m:1 driver_cd using $drivermatchfile, keep(match master) keepusing(driver_cd_loc) nogen


  rename ref_postcode postal
  merge m:1 postal using $commonpath/postalgeocode_extra, keep(match master) keepusing(latitude longitude) nogen

  gen ix = int((longitude - $sg_lon)*$kmperlon/($bwx_m/1000))
  gen iy = int((latitude - $sg_lat)*$kmperlat/($bwy_m/1000))
  gen it = int((ref_end_dt/1000/60 - `=td(1dec2016)'*24*60)/$bwt_mins)

  sort it ix iy
  rename driver_cd_loc driver_cd_loc_master

  if "`saveto'" != "" save "`saveto'", replace

end








capture program drop spatial_match
program define spatial_match
  syntax using/, date(integer)


  local datestr = `=string(`date',"%tdCCYYNNDD")'
  local nextdaystr = `=string(`date'+1,"%tdCCYYNNDD")'
  local nextnextdaystr = `=string(`date'+2,"%tdCCYYNNDD")'
  local locfile "vehicle_location_`datestr'_`nextdaystr'.dta"
  
  use * if inrange(ref_end_dt, `date'*24*60*60*1000 - $bwt_mins*60*1000, (`date'+1)*24*60*60*1000 + $bwt_mins*60*1000) using "`using'", clear
  tempfile dailymaster
  save `dailymaster'


  use $ivupath/`locfile', clear
  keep if status == 4
  gen jx = int((lon - $sg_lon)*$kmperlon/($bwx_m/1000))
  gen jy = int((lat - $sg_lat)*$kmperlat/($bwy_m/1000))
  gen jt = int((log_dt/1000/60 - `=td(1dec2016)'*24*60)/$bwt_mins)

  sort jt jx jy

  compress
  tempfile matchedfile
  save `matchedfile'

  tempfile resfile
  clear 
  save `resfile', replace emptyok

  forvalues dx=-1/1 {
    forvalues dy=-1/1 {
      forvalues dt=-1/1 {
        use `matchedfile'
        gen it = jt + `dt'
        gen ix = jx + `dx'
        gen iy = jy + `dy'

        joinby it ix iy using `dailymaster'

        drop if driver_cd == driver_cd_loc_master

        gen dt = abs(log_dt - ref_end_dt)
        bys id driver_cd_loc (dt): keep if _n == 1

        append using `resfile'
        save `resfile', replace

      }
    }
  }


  bys id driver_cd (dt): keep if _n == 1
  geodist latitude longitude lat lon, g(dist_km)

  bys id (dist_km): keep if _n == 1

  keep id driver_cd log_dt dist_km
  rename driver_cd driver_cd_loc
  merge m:1 driver_cd_loc using $drivermatchfile, keep(match master) keepusing(driver_cd) nogen

end


// tempfile masterfile resultfile
// prepare_master using $tmp/trips_merged_dec2feb_20190129_2_spl2, saveto("`masterfile'")

// clear
// save "`resultfile'", emptyok

// forvalues dayid=1/90 {
//   local date = `=`=td(1dec2016)'+`dayid'-1'

//   di "==============================================="
//   di "`dayid'"
//   di "==============================================="

//   spatial_match using "`masterfile'", date(`date')  
//   append using "`resultfile'"
//   save "`resultfile'", replace
// }

// use "`resultfile'", clear
// save $tmp/spatial_placebo_match, replace





if `dayid' == 999 {
  args dayid masterfile tripsfile
  prepare_master using "`tripsfile'", saveto("`masterfile'")
}
else {
  args dayid masterfile outdir
  local date = `=`=td(1dec2016)'+`dayid'-1'
  spatial_match using "`masterfile'", date(`date')  
  save "`outdir'/spatial_placebo_match_`dayid'", replace
}

