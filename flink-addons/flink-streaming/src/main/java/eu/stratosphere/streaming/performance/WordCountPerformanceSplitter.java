package eu.stratosphere.streaming.performance;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.util.PerformanceCounter;
import eu.stratosphere.util.Collector;

public class WordCountPerformanceSplitter extends FlatMapFunction<Tuple1<String>, Tuple1<String>> {

	private static final long serialVersionUID = 1L;

	private Tuple1<String> outTuple = new Tuple1<String>();

	 PerformanceCounter pCounter = new
	 PerformanceCounter("SplitterEmitCounter", 1000, 1000, 30000,
	 "/home/judit/strato/perf/broadcast4.csv");

	@Override
	public void flatMap(Tuple1<String> inTuple, Collector<Tuple1<String>> out) throws Exception {

		for (String word : inTuple.f0.split(" ")) {
			outTuple.f0 = word;
			// pTimer.startTimer();
			out.collect(outTuple);
			// pTimer.stopTimer();
			pCounter.count();
		}
	}

	// @Override
	// public String getResult() {
	// pCounter.writeCSV();
	// pTimer.writeCSV();
	// return "";
	// }

}
