#!/bin/bash

nodeDir1=/export
nodeDir2=/export1
nodeDir3=/export2
nodeDir4=/export3

cd /
mkdir /nodevol/log -p

for dir in $nodeDir1 $nodeDir2 $nodeDir3 $nodeDir4
	do 
		mkdir $dir/data
	done

mv cfg.json /nodevol
cd /nodevol
addr=`hostname -I`
sed -i "s/192.168.13.60/$addr/g" /nodevol/cfg.json

echo `hostname -I`
grep Ip cfg.json

