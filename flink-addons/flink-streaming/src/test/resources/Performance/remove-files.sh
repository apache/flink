#!/bin/bash
ssh strato@dell150.ilab.sztaki.hu '
for j in {101..142} 144 145;
do
	$(ssh dell$j "rm stratosphere-distrib/log/counter/*");
	$(ssh dell$j "rm stratosphere-distrib/log/timer/*");
	rm stratosphere-distrib/log/counter/*
	rm stratosphere-distrib/log/timer/*
done
'