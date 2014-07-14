#!/bin/bash
toDir=$1

if [ -d "${toDir}" ] ; then
	ssh strato@dell150.ilab.sztaki.hu '
	for j in {101..142} 144 145;
	do
		for i in $(ssh dell$j "ls stratosphere-distrib/log/counter/");
			do scp strato@dell$j:stratosphere-distrib/log/counter/$i stratosphere-distrib/log/all_tests/counter/$i-$j.csv;
		done
		for i in $(ssh dell$j "ls stratosphere-distrib/log/timer/");
			do scp strato@dell$j:stratosphere-distrib/log/timer/$i stratosphere-distrib/log/all_tests/timer/$i-$j.csv;
		done
		for i in $(ls stratosphere-distrib/log/counter/);
			do cp stratosphere-distrib/log/counter/$i stratosphere-distrib/log/all_tests/counter/$i-150.csv;
		done
		for i in $(ls stratosphere-distrib/log/timer/);
			do cp stratosphere-distrib/log/timer/$i stratosphere-distrib/log/all_tests/timer/$i-150.csv;
		done
	done
	'
	scp strato@dell150.ilab.sztaki.hu:stratosphere-distrib/log/all_tests/counter/* $toDir/counter/
	scp strato@dell150.ilab.sztaki.hu:stratosphere-distrib/log/all_tests/timer/* $toDir/timer/
else
	echo "USAGE:"
	echo "run <directory>"
fi