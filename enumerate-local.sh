#!/bin/bash

rm -r logs
mkdir logs
for m in 0 1
do
    for a in off fast strict
    do
        for w in t2 t3 t4
        do
            cargo +nightly run -- -w $w -m $m -a $a -o logs/$w-$m-$a.log
        done
    done
done

cargo +nightly run --bin log2csv -- -l