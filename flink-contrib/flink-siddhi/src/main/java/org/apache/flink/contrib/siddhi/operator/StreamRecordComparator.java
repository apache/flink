package org.apache.flink.contrib.siddhi.operator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Comparator;

public class StreamRecordComparator<IN> implements Comparator<StreamRecord<IN>>, Serializable {
	private static final long serialVersionUID = 1581054988433915305L;

	@Override
	public int compare(StreamRecord<IN> o1, StreamRecord<IN> o2) {
		if (o1.getTimestamp() < o2.getTimestamp()) {
			return -1;
		} else if (o1.getTimestamp() > o2.getTimestamp()) {
			return 1;
		} else {
			return 0;
		}
	}
}
