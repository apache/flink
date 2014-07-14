package eu.stratosphere.streaming.api.function;

import eu.stratosphere.api.java.tuple.Tuple;

/**
 * Dummy implementation of the SinkFunction writing every tuple to the standard
 * output. Used for print.
 * 
 * @param <IN>
 *            Input tuple type
 */
public class PrintSinkFunction<IN extends Tuple> extends SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(IN tuple) {
		System.out.println(tuple);
	}

}