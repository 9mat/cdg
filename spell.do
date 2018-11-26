// 26 Nov 2018: add first_lat first_lon last_lat last_lon to the output

set more off

args loc_dir dayid out_dir

local date = `=`=td(1dec2016)'+`dayid'-1'
local datestr = `=string(`date',"%tdCCYYNNDD")'
local nextdaystr = `=string(`date'+1,"%tdCCYYNNDD")'

use `loc_dir'/vehicle_location_`datestr'_`nextdaystr'.dta, clear

* points outside singapore land -- likely to be errors
* however, in terms of status, it does not matter
drop if lat < 1.23765 | lat > 1.47086
drop if lon < 103.60609 | lon > 104.044496
drop if lat < 1.277231 & lon > 103.87861
drop if lat > 1.399197 & lon > 103.932422
drop if lat > 1.443136 & lon > 103.872984

* new spell if (1) very first obs, or (2) status change, or (3) gap > 30 mins
bys vehicle_cd (log_dt): gen byte new_spell = _n==1 | status != status[_n-1] | (log_dt-log_dt[_n-1])/1000/60 > 30

* calculate the ditance of travel between two consecutive readings
bys vehicle_cd (log_dt): gen last_lat = lat[_n-1] if _n > 1
bys vehicle_cd (log_dt): gen last_lon = lon[_n-1] if _n > 1
geodist lat lon last_lat last_lon, g(distance) sphere

* drop unneeded variables to save memory space
drop last_lat last_lon

* spell number = cumulative of spell change indicator
bys vehicle_cd (log_dt): gen spell_num = sum(new_spell)

sort vehicle_cd spell_num log_dt

timer clear 1
timer on 1

* find the start and end time of each spell
* sum up the distance inbetween readings
* for now, assume the distance of the segment in between two speels belong to the later spell
* there are some adjustments that may need to be made, since BREAK and OFFLINE tend to be longer than other spell
fcollapse (min) spell_start_dt = log_dt ///
  (max) spell_end_dt = log_dt ///
  (sum) distance ///
  (first) status ///
  (first) first_lat = lat firt_lon = lon ///
  (last) last_lat = lat last_lon = lon, by(vehicle_cd spell_num)


timer off 1
timer list 1


format spell_start_dt spell_end_dt %tc

label define status_lbl 1 ARRIVED 2 BREAK 3 BUSY 4 FREE 5 NOSHOW 6 OFFLINE 7 ONCALL 8 PAYMENT 9 POB 10 STC
label values status status_lbl

keep vehicle_cd spell_start_dt spell_end_dt status distance first_lat first_lon last_lat last_lon
compress

* create the output folder, if it does not exist
capture mkdir `out_dir'

* output the spells
save `out_dir'/spellv2_`datestr'.dta, replace
