#!/bin/bash

##################################################
# sh automation.sh
##################################################

echo 'START'
trap break INT

source C:/ProgramData/Miniconda3/etc/profile.d/conda.sh
conda activate kiaps

find D:/project/PythonProjects/KIAPS_BATCH/data/surface/surface_LATE -type f -name '*.txt' -print0 |
    while IFS= read -r -d '' line
    do
        echo "$line"
        python modules/surface/surface_LATE.py --data "$line"
    done

#st=$(date -d "-9 hour -5 minute" "+%Y%m%d %H%M")
#data_time=${st:0:8}${st:9:4}
#echo "$data_time"
#touch -t 202204010101 first
#touch -t 202212312400 last
#find C:/Users/typeo/Downloads/* -newer first ! -newer last -print0 |
#    while read -r -d $'\0' file
#    do
#        echo "$file"
#    done

trap - INT
echo 'END'