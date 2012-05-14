package eu.stratosphere.pact.iterative.nephele.util;


import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

public abstract class PactRecordCollector implements Collector {

	private OutputCollectorV2 collector;

	public PactRecordCollector(OutputCollectorV2 collector) {
		this.collector = collector;
	}
	
	@Override
	public void collect(PactRecord record) {
		collectNextRecord(collector, record);
	}
	
	public abstract void collectNextRecord(OutputCollectorV2 collector,
			PactRecord record);

	@Override
	public void close() {
		collector.close();
	}

}
