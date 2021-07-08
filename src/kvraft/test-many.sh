#!/bin/bash

rm res -rf
mkdir res

for ((i = 0; i < 20; i++))
do

    for ((c = $((i*2)); c < $(( (i+1)*2)); c++))
    do                  #replace job name here
         (go test -run TestPersistPartitionUnreliable3A) &> ./res/$c &
         # sleep 5

    done

    sleep 20

    echo "finish 2 iterations."

done

sleep 5

grep -nr "FAIL.*raft.*" res
grep -nr "PASS" res
