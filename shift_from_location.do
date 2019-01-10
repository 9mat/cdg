args inpf outf

use "`inpf'", clear

// 2 BREAK 3 BUSY 6 OFFLINE
drop if inlist(status,2,3,6)

bys driver_cd (log_dt): gen double last_log_dt = log_dt[_n-1] if _n > 1
gen interval_mins = (log_dt  - last_log_dt)/1000/60
gen newshift = interval_mins > 6*60
bys driver_cd (log_dt): gen shiftnum = sum(newshift)
collapse (min) shift_start_dt = log_dt (max) shift_end_dt = log_dt , by(driver_cd shiftnum)
format shift_start_dt shift_end_dt %tc
compress

save "outf", replace
