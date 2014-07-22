package org.apache.flink.streaming.api.invokable.operator;

import java.util.Iterator;

public interface BatchIterator<IN> extends Iterator<IN> {
	public void reset();
}
