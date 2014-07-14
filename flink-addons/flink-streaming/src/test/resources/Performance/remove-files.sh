#!/bin/bash
echo REMOVING:
ssh strato@dell150.ilab.sztaki.hu '
for j in {101..142} 144 145;
do
	echo -n $j,
   	$(ssh dell$j "rm stratosphere-distrib/log/counter/*");
   	$(ssh dell$j "rm stratosphere-distrib/log/timer/*");
done

echo 150
rm stratosphere-distrib/log/counter/*
rm stratosphere-distrib/log/timer/*
rm stratosphere-distrib/log/all_tests/counter/*
rm stratosphere-distrib/log/all_tests/timer/*
'