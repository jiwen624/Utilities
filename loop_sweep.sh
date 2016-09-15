#!/bin/bash

trap 'trap - INT; kill -s HUP -- -$$' INT

while :
do
  for cnf in *.cnf
  do
    if [[ "$cnf" != "*.cnf" ]]
    then
      ./sweep.py ${cnf} -vv 2>&1 |tee -a log.loop_sweep & wait
    fi
  done
  sleep 60
done
