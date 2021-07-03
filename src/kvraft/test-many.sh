#!/bin/bash

rm res -rf
mkdir res

for ((i = 0; i < 10; i++))
do

    for ((c = $((i*5)); c < $(( (i+1)*5)); c++))
    do                  #replace job name here
         (time go test) &> ./res/$c &
         # sleep 5

    done

    sleep 380

    echo "finish 5 iterations."

done

sleep 50

grep -nr "FAIL.*raft.*" res
grep -nr "PASS" res