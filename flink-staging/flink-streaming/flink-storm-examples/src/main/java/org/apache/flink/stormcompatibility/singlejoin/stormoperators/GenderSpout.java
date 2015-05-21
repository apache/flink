package org.apache.flink.stormcompatibility.singlejoin.stormoperators;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.flink.stormcompatibility.util.AbstractStormSpout;

public class GenderSpout extends AbstractStormSpout {
	private int counter = 9;
	private Fields outFields;

	public GenderSpout(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void nextTuple() {
		if (counter >= 0) {
			this.collector.emit(new Values(counter, counter + 20));
			counter--;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}
}
