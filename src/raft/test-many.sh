#!/bin/bash

rm res -rf
mkdir res

for ((i = 0; i < 40; i++))
do

    for ((c = $((i*3)); c < $(( (i+1)*3)); c++))
    do                  #replace job name here
         (go test -run 2B) &> ./res/$c &
         # sleep 5

    done

    sleep 50

    echo "finish 3 iterations."

done

sleep 5

grep -nr "FAIL.*raft.*" res
grep -nr "PASS" res