#!/bin/bash

rm res -rf
mkdir res

for ((i = 0; i < 400; i++))
do

    for ((c = $((i*5)); c < $(( (i+1)*5)); c++))
    do                  #replace job name here
         (go test) &> ./res/$c &
         # sleep 5

    done

    sleep 390

    echo "finish 2 iterations."

done

sleep 60

grep -nr "FAIL:" res
grep -nr "PASS" res
