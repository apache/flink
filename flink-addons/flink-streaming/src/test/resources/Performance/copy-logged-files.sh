#!/bin/bash
toDir=$1

if [ -d "${toDir}" ] ; then
	for j in {0..2}
	do
		for i in $(ssh strato@192.168.201.16$j "ls strato-dist/log/counter/");
			do scp strato@192.168.201.16$j:strato-dist/log/counter/$i $toDir/counter/$i-0$(expr $j + 1).csv;
		done
		for i in $(ssh strato@192.168.201.16$j "ls strato-dist/log/timer/");
			do scp strato@192.168.201.16$j:strato-dist/log/timer/$i $toDir/timer/$i-0$(expr $j + 1).csv;
		done
	done
else
	echo "USAGE:"
	echo "run <directory>"
fi
