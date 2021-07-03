#!/bin/bash

rm results -rf
mkdir results

for ((i = 0; i < 90; i++))
do

    for ((c = $((i*5)); c < $(( (i+1)*5)); c++))
    do                  #replace job name here
         (go test) &> ./results/$c &
         # sleep 5

    done

    sleep 120

    echo "finish 5 iterations."

done

sleep 5

grep -nr "FAIL: " results
grep -nr "PASS" results