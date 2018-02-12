#!/bin/bash
./client -sAddr="192.168.183.72:54321" -opt="write" -nodes=2 -nAddrs="192.168.183.72:54321/192.168.183.74:54321/192.168.183.75:54321" -vols=100 -log=1 -reqs=20000 -threads=1
