set more off
set rmsg on

args inputf outdir theta theta_cns

use "`inputf'", clear
  
bys driver_cd shift_num (ref_end_dt): gen double discounted_cum_cns_shock = `theta_cns'*cns_shock if _n == 1
bys driver_cd shift_num (ref_end_dt): replace discounted_cum_cns_shock = `theta_cns'*(discounted_cum_cns_shock[_n-1]+cns_shock) if _n > 1
bys driver_cd shift_num (ref_end_dt): gen double ref_income = initial_ref_income if _n == 1
bys driver_cd shift_num (ref_end_dt): replace ref_income = `theta'*ref_income[_n-1] + (1-`theta')*updated_expectation + cum_cns_shock - discounted_cum_cns_shock if _n > 1
gen net_working_loss =  min(0, pred_next_income - ref_income) -  min(0, cum_income - ref_income)


clogit quit pred_trip_fare net_working_loss c.cum_hours##c.pred_trip_mins i.trip_end_dow i.trip_end_hour, group(driver_cd)

file open myfile using "`outdir'/adapt_ref_LL`theta'_`theta_cns'.txt", write replace
file write myfile "`theta'" _tab "`theta_cns'" _tab "`e(ll)'"
file close myfile
