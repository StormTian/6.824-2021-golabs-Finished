#!/bin/bash

rm res -rf
mkdir res

for ((i = 0; i < 5; i++))
do

    for ((c = $((i*2)); c < $(( (i+1)*2)); c++))
    do                  #replace job name here
         (go test -run 2A -race) &> ./res/$c &
         # sleep 15

    done

    sleep 10

    echo "finish 2 iterations."

done

sleep 5

grep -nr "FAIL.*raft.*" res
grep -nr "PASS" res