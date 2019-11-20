// 20191119
// by HL
// count number of jobs within a certain radius and mins from the end of each trips
// to be used as a proxy for market demand/market conditions

set more off

include definepath

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


// divide Singapore into grid of 1km*1km and divide the sample period into windows of 5 minutes
// assuming the radius and time gap is less than 1km and 5 minutes
// we only need to match points in grid cell with 9 neighbor cells and 3 neighbor time windows (aka 27 neighbor cell-windows)

capture program drop countnearbybookings
program define countnearbybookings
  syntax using/, radius_m(real) buffer_mins(real) date(int) [saveto(string)]

  tempfile monthlymatchedjobs monthlymaster resfile

  // prepared jobs to be matched
  use id job_status trip_start_dt booking_dt req_pickup_dt pickup_postcode using "`using'", clear

  gen double j_dt = trip_start_dt if job_status == .
  replace j_dt = max(req_pickup_dt - 15*1000*60, booking_dt) if job_status != .
  keep if inrange(j_dt, cofd(`date') - `buffer_mins'*1000*60, cofd(`date'+1) + `buffer_mins'*1000*60)

  rename pickup_postcode postal
  fmerge m:1 postal using $commonpath/postalgeocode_extra, keep(match master) keepusing(latitude longitude) nogen

  gen jx = int((longitude - $sg_lon)*$kmperlon/($bwx_m/1000))
  gen jy = int((latitude - $sg_lat)*$kmperlat/($bwy_m/1000))
  gen jt = int((j_dt/1000/60 - td(1dec2016)*24*60)/$bwt_mins)

  keep  jt jx jy job_status latitude longitude j_dt
  rename (job_status latitude longitude) (j_status j_lat j_lon)
  sort jt jx jy

  compress
  save "`monthlymatchedjobs'", replace




  // prepare master files i.e. trips that we need to count the number of nearby jobs
  use id job_status ref_end_dt ref_postcode if inrange(ref_end_dt, cofd(`date'), cofd(`date'+1)-1) using "`using'", clear
  rename ref_postcode postal
  fmerge m:1 postal using $commonpath/postalgeocode_extra, keep(match master) keepusing(latitude longitude) nogen

  gen ix = int((longitude - $sg_lon)*$kmperlon/($bwx_m/1000))
  gen iy = int((latitude - $sg_lat)*$kmperlat/($bwy_m/1000))
  gen it = int((ref_end_dt/1000/60 - td(1dec2016)*24*60)/$bwt_mins)

  sort it ix iy

  compress
  save `monthlymaster', replace


  timer clear 1


  clear
  save "`resfile'", replace emptyok

  di "============================================"
  di `"`=string(`date', "%tdCYND")'"'
  di "============================================"

  local suffix "`radius_m'm`buffer_mins'min"

  // loops over neighborimg map cells and time windows

  forvalues dx=-1/1 {
    forvalues dy=-1/1 {
      forvalues dt=-1/1 {
        di "dx `dx' dy `dy' dt `dt'"

        timer on 1

        use `monthlymatchedjobs', clear

        gen ix = jx + `dx'
        gen iy = jy + `dy'
        gen it = jt + `dt'

        drop if inlist(., it, ix, iy)

        joinby it ix iy using `monthlymaster'

        keep if abs(ref_end_dt - j_dt)/1000/60 < `buffer_mins' 

        geodist latitude longitude j_lat j_lon, g(dist_km)
        keep if dist_km < `radius_m'/1000

        gen bookings`suffix' = j_status != .
        gen completed`suffix' = j_status == "COMPLETED":job_status
        gen streethails`suffix' = j_status == .

        collapse (sum) bookings`suffix' completed`suffix' streethails`suffix', by(id)

        timer off 1
        timer list 1

        append using "`resfile'"
        save "`resfile'", replace
      }
    }
  }
  use "`resfile'", clear
  collapse (sum) bookings`suffix' completed`suffix' streethails`suffix', by(id)

  if "`saveto'" != "" save "`saveto'", replace

end


// countnearbybookings using $cdgpath/trips_merged_dec2feb_20190129.dta, radius_m(1000) buffer_mins(5) date(`=td(1dec2016)')
// exit


if "`c(hostname)'" == "musang01" | strpos("`c(hostname)'", "comp") > 0 | strpos("`c(hostname)'", "hpc") > 0 {
  args jid tripfile outdir rr mm

  countnearbybookings using "`tripfile'", radius_m(`rr') buffer_mins(`mm') date(`=td(1dec2016)+`jid'-1') saveto("`outdir'/nearby`rr'm`mm'min`jid'.dta")
}

else {
  tempfile resfile
  clear
  save "`resfile'", replace emptyok

  forvalues mm=tm(2016m12)/tm(2017m2) {
    countnearbybookings using "`tripfile'", radius_m(1000) buffer_mins(5) mm(`mm')

    append using "`resfile'"
    save "`resfile'", replace
  }

  use "`resfile'", clear
  save $tmp/nearby1000m5min, replace
}
