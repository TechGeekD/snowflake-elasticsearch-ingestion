#!/bin/bash

# 6:25:00 PM - 7:25:00 PM - 10M
# 7:25:00 PM - 8:28:00 PM - 10M
# 8:28:00 PM - 9:32:00 PM - 10M
for a in $(seq 16000000 1000000 17000000)
do
	a=`expr $a + 1`
    echo $a
	date +"%T"
    python push_es.py $a &
	sleep 20s
	echo "next batch starts"
done
