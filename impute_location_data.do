// global trips_folder "E:/cdg_data/generated/"
// global until 1jan2017
// globle common_folder "D:/Dropbox/work/data/dta"

args trips_folder common_folder until
global trips_folder "`trips_folder'"
global until "`until'"
global common_folder "`common_folder'"

use "$trips_folder/trips_merged_dec2feb_everything.dta" if dofc(ref_start_dt) < td($until), clear
merge 1:1 id using "$trips_folder/trips_dec2feb_nearmrg.dta", keep(match master) nogen

cap drop ref_end_dt
gen double ref_end_dt = trip_end_dt if job_status == . | job_status == "COMPLETED":job_status
replace ref_end_dt = job_start_dt + oncall_mins*60*1000 if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)

// cap drop ref_start_dt2
// gen double ref_start_dt2 = trip_start_dt if job_status == . | job_status == "COMPLETED":job_status
// replace ref_start_dt2 = job_start_dt if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)

// bys driver_cd shift_num (ref_end_dt): gen double next_start = ref_start_dt2[_n+1] if _n < _N
// bys driver_cd shift_num (ref_end_dt): gen double next_end = ref_end_dt[_n+1] if _n < _N
// format ref_end_dt ref_start_dt2 next_start next_end %tc

// count if ref_end_dt == .
// // 40775
// count if ref_end_dt != . & ref_end_dt > next_start
// // 34950


cap drop shift_end_dt
gen double shift_end_dt_tmp = spell_end_dt if quit
bys driver_cd shift_num: egen double shift_end_dt = max(shift_end_dt_tmp)
drop shift_end_dt_tmp

// bys driver_cd shift_num: egen double shift_end_dt = max(ref_end_dt)

format ref_start_dt shift_end_dt %tc

cap drop cum_hours
gen cum_hours = (ref_end_dt - shift_start_dt)/1000/60/60

cap drop remaining_mins
gen remaining_mins = (shift_end_dt - ref_end_dt)/1000/60

cap drop remaining_wage
gen remaining_wage = remaining_income/(remaining_mins/60)


cap drop working_mins
gen double working_mins = (trip_end_dt - trip_start_dt)/1000/60
replace working_mins = 0 if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)
replace working_mins = working_mins + oncall_mins if job_status != .


bys driver_cd shift_num (ref_end_dt): gen cum_working_mins = sum(working_mins)
bys driver_cd shift_num: egen total_working_mins = total(working_mins)
gen remaining_working_mins = total_working_mins - cum_working_mins
gen remaining_idle_pct = (1 - remaining_working_mins/remaining_mins)*100


cap drop _merge

geonear id end_lat end_lon using "$common_folder/postalgeocode.dta", n(postal latitude longitude) near(1) g(matched_dest_postcode)


replace dest_postcode = matched_dest_postcode if (job_status == . | job_status == "COMPLETED":job_status) & (dest_postcode == . | dest_postcode <= 10000)

gen ref_postcode = dest_postcode if inlist(job_status, ., "COMPLETED":job_status)
replace ref_postcode = matched_dest_postcode if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)


save "$trips_folder/trips_merged_nearmrg_dec2feb.dta", replace

// reghdfe remaining_wage dv_cancellation dv_noshow cum_hours if remaining_mins > 60, absorb(driver_cd hour#dow#zonecode date)
