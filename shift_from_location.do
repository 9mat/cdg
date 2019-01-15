args inpf outf

use "`inpf'", clear

// quality control: drop spell with duration < 5 mins and more than 10 mins way from other spell
bys vehicle_cd driver_cd (log_dt): gen gapmins = (log_dt - log_dt[_n-1])/1000/60 if _n > 1
gen newspell = gapmins > 10
bys vehicle_cd driver_cd (log_dt): gen spellnum = sum(gapmins)
bys vehicle_cd driver_cd shiftnum: egen double speLL_start_dt = min(log_dt)
bys vehicle_cd driver_cd shiftnum: egen double speLL_end_dt = max(log_dt)
gen spell_mins = (speLL_end_dt - speLL_start_dt)/1000/60
drop if spell_mins < 5
drop gapmins newspell spellnum speLL_start_dt speLL_end_dt spell_mins



// 2 BREAK 3 BUSY 6 OFFLINE
drop if inlist(status,2,3,6)

bys driver_cd (log_dt): gen double last_log_dt = log_dt[_n-1] if _n > 1
gen interval_mins = (log_dt  - last_log_dt)/1000/60
gen newshift = interval_mins > 6*60
bys driver_cd (log_dt): gen shiftnum = sum(newshift)
collapse (min) shift_start_dt = log_dt (max) shift_end_dt = log_dt , by(driver_cd shiftnum)
format shift_start_dt shift_end_dt %tc
compress

save "`outf'", replace
