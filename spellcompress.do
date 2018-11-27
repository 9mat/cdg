args folder

set more off

*local folder "E:/cdg_data/generated"
*local folder "/NAS/project01/chujunhong_SPIRE"

use "`folder'/spell.dta", clear

gen byte combstatus = 0 if status == "OFFLINE":status_lbl
replace combstatus = 1 if inlist(status, "FREE":status_lbl, "BUSY":status_lbl, "BREAK":status_lbl)
replace combstatus = 2 if inlist(status, "POB":status_lbl, "STC":status_lbl, "PAYMENT":status_lbl)
replace combstatus = 3 if inlist(status, "ONCALL":status_lbl, "ARRIVED":status_lbl, "NOSHOW":status_lbl)


label define combstatus 0 "OFFLINE" 1 "NO JOB" 2 "WITH PASSENGER" 3 "TO PASSENGER"
label values combstatus combstatus

compress

bys vehicle_cd (spell_start_dt): gen newspell = combstatus != combstatus[_n-1] | _n == 1
bys vehicle_cd (spell_start_dt): gen spellnum = sum(newspell)

compress

sort vehicle_cd spell_start_dt
fcollapse (min) spell_start_dt (max) spell_end_dt (sum) distance (first) combstatus first_lat first_lon (last) last_lat last_lon, by(vehicle_cd spellnum)


save "`folder'/combspell.dta", replace
