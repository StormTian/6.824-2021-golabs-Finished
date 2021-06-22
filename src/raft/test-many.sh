#!/bin/bash

rm res -rf
mkdir res

for ((i = 0; i < 10; i++))
do

    for ((c = $((i*3)); c < $(( (i+1)*3)); c++))
    do                  #replace job name here
         (go test -run 2D) &> ./res/$c &
         # sleep 5

    done

    sleep 120

    echo "finish 3 iterations."

done

sleep 20

grep -nr "FAIL.*raft.*" res
grep -nr "PASS" res