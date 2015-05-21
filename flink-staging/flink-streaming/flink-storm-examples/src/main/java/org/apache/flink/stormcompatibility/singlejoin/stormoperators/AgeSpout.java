package org.apache.flink.stormcompatibility.singlejoin.stormoperators;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.flink.stormcompatibility.util.AbstractStormSpout;

public class AgeSpout extends AbstractStormSpout {
	private static final long serialVersionUID = -4008858647468647019L;

	private int counter = 0;
	private String gender;
	private Fields outFields;

	public AgeSpout(Fields outFields) {
		this.outFields = outFields;
	}

	@Override
	public void nextTuple() {
		if (this.counter < 10) {
			if (counter % 2 == 0) {
				gender = "male";
			} else {
				gender = "female";
			}
			this.collector.emit(new Values(counter, gender));
			counter++;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}

}
