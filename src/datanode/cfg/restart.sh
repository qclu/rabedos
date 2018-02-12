#!/bin/bash

log=$1

cd /nodevol
killall ds_node
cp cmd ds_node
sleep 2
> nohup.out
ps -ef|grep ds_node

if log==1 then
	rm log/*
fi

sleep 2
nohup ./ds_node -c=cfg.json &
ps -ef|grep ds_node
cd /nodevol/log

