#!/usr/bin/bash

# --- #
# script to extract long time series from FROST
# as FROST cannot handle large data requests

# usage: 
# ./extract_data.sh 2016-01-01 2023-09-1 goliat /home/${USER}/tmp_printobs_goliat/data 
# --- #

start=$1
end=$2
station=$3
format=$4
path=$5

start=$(date -d $start +%Y%m%d)
end=$(date -d $end +%Y%m%d)

while [ ${start} -le ${end} ]
do
        echo $start
        tmp=$(date -d"$start + 1 month" +"%Y%m%d")
        echo $tmp
        poetry run printobs -s ${station} -sd ${start} -ed ${tmp}\
	       	-w ${format} -p ${path}/${station}_${start}_${tmp}.${format}
        start=$(date -d"$start + 1 month" +"%Y%m%d")
done
