set more off
global spellfolder "F:/CDGData"
global tripsfolder "E:/cdg_data/generated"
global tmp "F:/Temp"



// label define combstatus 0 "OFFLINE" 1 "NO JOB" 2 "WITH PASSENGER" 3 "TO PASSENGER" 4 "NOSHOW"

use if combstatus == 2 & dofc(spell_start_dt) <= td(3dec2016) using "$spellfolder/combspellv2.dta", clear
rename vehicle_cd vehicle_cd_loc
merge m:1 vehicle_cd_loc using "$tripsfolder/vehicle_cd_match.dta", keep(master match) nogen
drop if vehicle_cd == .
drop vehicle_cd_loc

// tempfile pobspell
// save "`pobspell'"
save $tmp/pobspell.dta, replace






use id trip_start_dt vehicle_cd job_status trip_end_dt using "$tripsfolder/trips_merged_dec2feb_everything.dta" if dofc(trip_start_dt) < td(3dec2016), clear
keep if job_status == . | job_status == "COMPLETED":job_status
count

gen double spell_end_dt = trip_end_dt
nearmrg vehicle_cd using $tmp/pobspell, nearvar(spell_end_dt) keep(match master) nogen genmatch(dest_dt)

rename first_lat pickup_lat
rename first_lon pickup_lon
rename last_lat dest_lat
rename last_lon dest_lon
rename spell_start_dt pickup_dt

gen discrepancy = abs(dest_dt - spell_end_dt)/1000/60
sum discrepancy,d
sum discrepancy if job_status == ., d
sum discrepancy if job_status != ., d

bys vehicle_cd spellnum: gen dup = _n
count if dup > 1
tab dup if discrepancy < 10


keep id pickup_lat pickup_lon dest_lat dest_lon pickup_dt dest_dt spellnum vehicle_cd spell_start_dt
compress
count

save $tmp/match_pob, replace






// label define combstatus 0 "OFFLINE" 1 "NO JOB" 2 "WITH PASSENGER" 3 "TO PASSENGER" 4 "NOSHOW"

use if combstatus == 3 & dofc(spell_start_dt) < td(3dec2016) using "$spellfolder/combspellv2.dta", clear
rename vehicle_cd vehicle_cd_loc
merge m:1 vehicle_cd_loc using "$tripsfolder/vehicle_cd_match.dta", keep(master match) nogen
drop if vehicle_cd == .
drop vehicle_cd_loc

// tempfile oncallspell
// save "`oncallspell'"
save $tmp/oncallspell.dta, replace




use id trip_start_dt vehicle_cd job_status using "$tripsfolder/trips_merged_dec2feb_everything.dta" if dofc(trip_start_dt) < td(3dec2016), clear
keep if job_status == "COMPLETED":job_status
count

gen double spell_end_dt = trip_start_dt
nearmrg vehicle_cd using $tmp/oncallspell, nearvar(spell_end_dt) limit(1000*60) keep(match master) nogen

rename first_lat oncall_lat
rename first_lon oncall_lon
rename spell_start_dt oncall_dt

keep id oncall_lon oncall_lat oncall_dt spellnum vehicle_cd
compress
count

save $tmp/match_oncall1, replace




use id req_pickup_dt vehicle_cd job_status using "$tripsfolder/trips_merged_dec2feb_everything.dta" if dofc(req_pickup_dt) < td(3dec2016), clear
keep if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)
count

gen double spell_start_dt = req_pickup_dt
nearmrg vehicle_cd using $tmp/oncallspell, nearvar(spell_start_dt) limit(1000*60*10) keep(match master) nogen

rename first_lat oncall_lat
rename first_lon oncall_lon
rename spell_start_dt oncall_dt
rename last_lat cns_lat
rename last_lon cns_lon
rename spell_end_dt cns_dt

keep id oncall_lat oncall_lon oncall_dt cns_lat cns_lon cns_dt spellnum vehicle_cd
compress
count

save $tmp/match_oncall2, replace



// label define combstatus 0 "OFFLINE" 1 "NO JOB" 2 "WITH PASSENGER" 3 "TO PASSENGER" 4 "NOSHOW"

use if combstatus == 1 & dofc(spell_start_dt) < td(3dec2016) using "$spellfolder/combspellv2.dta", clear
rename vehicle_cd vehicle_cd_loc
merge m:1 vehicle_cd_loc using "$tripsfolder/vehicle_cd_match.dta", keep(master match) nogen
drop if vehicle_cd == .
drop vehicle_cd_loc

// tempfile oncallspell
// save "`oncallspell'"
save $tmp/freespell.dta, replace




use $tmp/match_pob
merge 1:1 id using $tmp/match_oncall1, nogen
merge 1:1 id using $tmp/match_oncall2, nogen


gen double spell_start_dt = dest_dt if dest_dt != .
replace spell_start_dt = cns_dt if cns_dt != .

nearmrg vehicle_cd using $tmp/freespell, nearvar(spell_start_dt) upper keep(match master) nogen limit(1000*60)





clear

timer clear 1
timer on 1

tempfile realtime

forvalues d = `=td(1dec2016)'/`=td(3dec2016)' {
  preserve
  di `"`=string(`d'+1,"%tdCYND")'"'
  use if inlist(status, 8, 9, 10) using `"F:/CDGData/vehicle_location/vehicle_location_`=string(`d',"%tdCYND")'_`=string(`d'+1,"%tdCYND")'.dta"', clear
  save "`realtime'", replace
  restore
  append using "`realtime'"
}

drop if vehicle_cd == .
drop if log_dt == .
bys vehicle_cd log_dt: keep if _n ==1
save "`realtime'", replace


// use if inlist(status, 8, 9, 10) using F:/CDGData/vehicle_location/vehicle_location_20161201_20161202.dta, clear
// drop if vehicle_cd == .
// drop if log_dt == .
// bys vehicle_cd log_dt: keep if _n ==1
// tempfile realtime
// save "`realtime'"

use id trip_end_dt vehicle_cd job_status using "$tripsfolder/trips_merged_dec2feb_everything.dta" if dofc(trip_end_dt) < td(3dec2016), clear
count

keep if job_status == . | job_status == "COMPLETED":job_status
merge m:1 vehicle_cd using "$tripsfolder/vehicle_cd_match.dta", keep(master match) nogen
drop vehicle_cd
rename vehicle_cd_loc vehicle_cd
gen double log_dt = trip_end_dt

nearmrg vehicle_cd using "`realtime'", nearvar(log_dt) keep(match master) genmatch(match_dt)
count

timer off 1
timer list 1
