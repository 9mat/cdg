set more off

// global spellfolder "E:/cdg_data/generated"
// global tripsfolder "E:/cdg_data/generated"
// global tmp "F:/Temp"

global spellfolder "/NAS/project01/chujunhong_SPIRE"
global tripsfolder "/NAS/project01/chujunhong_SPIRE/trips"
global tmp "/NAS/project01/chujunhong_SPIRE/tmp"

*******************************************************************************
* generate id to keep tracks of observations across tables
// use E:/cdg_data/generated/trips_merged_dec2feb_everything.dta, clear
// gen long id = _n
// compress
// save E:/cdg_data/generated/trips_merged_dec2feb_everything.dta, replace



*******************************************************************************
* match vehicle codes between trip data and location data
// use E:/cdg_data/generated/trips_merged_dec2feb_everything.dta, clear
// keep vehicle_cd
// duplicates drop
// decode vehicle_cd, g(vehicle_id)
// rename vehicle_cd vehicle_cd_trips

// merge 1:1 vehicle_id using E:/cdg_data/generated/unique_veh_cd.dta, keep(match) nogen
// rename vehicle_cd vehicle_cd_loc
// rename vehicle_cd_trips vehicle_cd
// keep vehicle_cd vehicle_cd_loc

// notes: "match between the vehicle codes used in trip/booking data and those in location data"
// save E:/cdg_data/generated/vehicle_cd_match.dta, replace


use "$spellfolder/combspell.dta", clear
// keep if dofc(spell_start_dt) < td(1jan2017)

label define combstatus 0 "OFFLINE" 1 "FREE" 2 "POB" 3 "ONCALL"
label values combstatus combstatus

* serving customer: POB or ONCALL
gen byte dv_serving = inlist(combstatus, "POB":combstatus, "ONCALL":combstatus)

* new job: transition from free to serving a new customer
bys vehicle_cd (spell_start_dt): gen byte newjob = _n==1 | (dv_serving &  ~dv_serving[_n-1]) | combstatus[_n-1] == "OFFLINE":combstatus

* special case: transition from POB to ONCALL
bys vehicle_cd (spell_start_dt): replace newjob = 1 if combstatus == "ONCALL":combstatus & combstatus[_n-1] == "POB":combstatus & _n>1

* construct job_id by accumulate number of transition to new job
bys vehicle_cd (spell_start_dt): gen job_id = 1 if _n == 1
bys vehicle_cd (spell_start_dt): replace job_id = job_id[_n-1] + newjob if _n > 1

separate distance, by(combstatus)
separate spell_start_dt, by(combstatus)
separate spell_end_dt, by(combstatus)

//exit

collapse (min) spell_start_dt (max) spell_end_dt* (sum) distance*, by(vehicle_cd job_id)

* a super-spell consists of 3 segments: oncall, pob and free (free refers to the time between 2 jobs)
* a completed booking will have all 3 segments
* a street hail will have pob and maybe free but not oncall
* a C&NS will have oncall and maybe free but not pob
* there can also be offline segment if the super-spell is the last of the shift

* indicate if there is a pob/oncall/free segment in the super-spell
gen dv_no_oncall = spell_end_dt3 == .
gen dv_no_pob = spell_end_dt2 == .
gen dv_no_free = spell_end_dt1 == .


* the end of the job, aka the end of the pob spell for completed booking and street hail trips
* or the end of the oncall spell for C&NS
gen double jobend_dt = spell_end_dt2
replace jobend_dt = spell_end_dt3 if spell_end_dt2 ==.

* impute the end time of a spell by the the end time of the previous one if the current spell is missing
* this is to simplify the spell duration caculation (oncall_mins, pob_mins and free_mins) below
replace spell_end_dt3 = spell_start_dt if spell_end_dt3 == .
replace spell_end_dt2 = spell_end_dt3 if spell_end_dt2 == .
replace spell_end_dt1 = spell_end_dt2 if spell_end_dt1 == .

* spell duration in minutes (will be zero if the respective spell is missing)
gen oncall_mins = (spell_end_dt3 - spell_start_dt)/1000/60
gen pob_mins = (spell_end_dt2 - spell_end_dt3)/1000/60
gen free_mins = (spell_end_dt1 - spell_end_dt2)/1000/60


rename distance1 free_km
rename distance2 pob_km
rename distance3 oncall_km

gen dv_offline = spell_end_dt0 != .

keep vehicle_cd free_mins free_km oncall_mins oncall_km pob_mins jobend_dt pob_km dv_no_oncall dv_no_pob dv_no_free dv_offline spell_start_dt


drop if dv_no_pob & dv_no_oncall

foreach x in oncall free pob {
  replace `x'_km = . if dv_no_`x'
  replace `x'_mins = . if dv_no_`x'
}

drop dv_no_*


format jobend_dt %tc


// gen date = dofc(jobend_dt)
// gen hour = hh(jobend_dt)
// gen dow = dow(date)

compress

// gen dv_cns = oncall_mins != . & pob_mins == .

// replace dv_cns = . if oncall_mins == . & pob_mins == .

// gen free_kmh = free_km/(free_mins/60)

// drop if free_kmh < 0





// exit

rename vehicle_cd vehicle_cd_loc
merge m:1 vehicle_cd_loc using "$tripsfolder/vehicle_cd_match.dta", keep(master match) nogen
drop vehicle_cd_loc
gen long spellid = _n

// bys vehicle_cd (spell_start_dt): gen prestatus = combstatus[_n-1]

// bys vehicle_cd (spell_start_dt): gen nextstatus = combstatus[_n+1]
// bys vehicle_cd (spell_start_dt): gen nextdistance = distance[_n+1]
// bys vehicle_cd (spell_start_dt): gen double nextenddt = spell_end_dt[_n+1]

// bys vehicle_cd (spell_start_dt): gen nextnextstatus = combstatus[_n+2]
// bys vehicle_cd (spell_start_dt): gen nextnextdistance = distance[_n+2]
// bys vehicle_cd (spell_start_dt): gen double nextnextenddt = spell_end_dt[_n+2]

// label define combstatus 0 "OFFLINE" 1 "FREE" 2 "POB" 3 "ONCALL"
// label values combstatus nextstatus nextnextstatus combstatus

// keep if inlist(combstatus, "POB":combstatus, "ONCALL":combstatus)
// drop if combstatus == "POB":combstatus & prestatus == "ONCALL":combstatus

// * For completed street hail, the order witll be: with PASSENGER -> No job
// * For C&NS, the order will be: ONCALL -> no job
// gen double nextjob_dt = nextenddt if nextstatus == "FREE":combstatus

// * for completed booking, the order will be: ONCALL -> POB -> FREE
// replace nextjob_dt = nextnextenddt if combstatus == "ONCALL":combstatus & nextstatus == "POB":combstatus & nextnextstatus == "FREE":combstatus

// * special case: the next trip start right after this trip end
// replace nextjob_dt = spell_end_dt if nextstatus == "ONCALL":combstatus


// * street hail trips
// gen double jobend_dt = spell_end_dt if combstatus == "POB":combstatus

// * completed bookings
// replace jobend_dt = nextenddt if combstatus == "ONCALL":combstatus & nextstatus == "POB":combstatus

// * C&NS
// replace jobend_dt = spell_end_dt if combstatus == "ONCALL":combstatus & nextstatus == "FREE":combstatus

// exit

// tempfile spelltmp
save "$tmp/spelltmp.dta", replace

use "$tripsfolder/trips_merged_dec2feb_everything.dta", clear
// keep if dofc(ref_start_dt) < td(1jan2017)

keep id trip_start_dt trip_end_dt booking_dt req_pickup_dt job_status vehicle_cd driver_cd
append using "$tmp/spelltmp.dta"

gen double ref_dt = spell_start_dt if spellid != . & job_status == .
replace ref_dt = req_pickup_dt if job_status != . & spellid == .
replace ref_dt = trip_start_dt if job_status == . & spellid == .
assert ref_dt != .

format ref_dt %tc

sort vehicle_cd ref_dt

bys vehicle_cd (ref_dt): gen double time_to_next = ref_dt[_n+1] - ref_dt
bys vehicle_cd (ref_dt): gen double time_to_last = ref_dt - ref_dt[_n-1] if _n > 1

gen take_next = time_to_next < time_to_last if id != . & time_to_last != . & time_to_next != .

* street hail trips: status should only change after trip start, so always take next
replace take_next = 1 if job_status == .

* immediate booking: status should only change after booking is made, so always take next
replace take_next = 1 if booking_dt != . & booking_dt == req_pickup_dt

foreach x in pob_km free_km pob_mins free_mins oncall_km oncall_mins spell_start_dt jobend_dt dv_offline {
  bys vehicle_cd (ref_dt): replace `x' = `x'[_n+1] if id != . & id[_n+1] == . & take_next == 1
  bys vehicle_cd (ref_dt): replace `x' = `x'[_n-1] if id != . & id[_n-1] == . & take_next == 0
}


keep if id != .

save "$tripsfolder/trips_dec2feb_spells.dta", replace
