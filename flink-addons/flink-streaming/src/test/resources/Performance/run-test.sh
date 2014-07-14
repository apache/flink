#!/bin/bash
toDir=$1
testParams=$2
if [ -d "${toDir}" ] ; then
	echo "removing files"
	./remove-files.sh

	paramsWithSpace="${testParams//_/ }"

	rm -r $toDir/$testParams/*;
	mkdir $toDir/$testParams;
	mkdir $toDir/$testParams/counter;
	mkdir $toDir/$testParams/timer;

	ssh -n strato@dell150.ilab.sztaki.hu "./stratosphere-distrib/bin/stratosphere run -j ./stratosphere-distrib/lib/stratosphere-streaming-0.5-SNAPSHOT.jar -c eu.stratosphere.streaming.examples.wordcount.WordCountLocal -a cluster ${paramsWithSpace}"
	echo "job finished"

	echo "copying"
	./copy-files.sh $toDir/$testParams
else
	echo "USAGE:"
	echo "run <directory> <test params separated by _>"
fi