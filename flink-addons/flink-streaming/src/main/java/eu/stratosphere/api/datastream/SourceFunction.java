package eu.stratosphere.api.datastream;

import java.io.Serializable;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.StreamCollector;

public abstract class SourceFunction<OUT extends Tuple> implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract void invoke(StreamCollector<Tuple> collector);

}
