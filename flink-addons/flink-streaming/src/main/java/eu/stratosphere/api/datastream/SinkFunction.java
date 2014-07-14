package eu.stratosphere.api.datastream;

import java.io.Serializable;

import eu.stratosphere.api.java.tuple.Tuple;

public abstract class SinkFunction<IN extends Tuple> implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract void invoke(IN tuple);

}
