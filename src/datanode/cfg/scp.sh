#!/bin/bash

dst=$1
dir=$2
ip=$3

ips=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
ip2=(69 70 71 72 73 74 75 76)


if [ $ip -ne 1 ] 
then
  ips=(${ip2[@]})
fi

echo ${ips[@]}

for ip in ${ips[*]}
  do 
  {
	scp $dst $ip:$dir
  }&
  done
  wait

