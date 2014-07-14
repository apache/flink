package eu.stratosphere.api.datastream;

import eu.stratosphere.types.TypeInformation;

public abstract class StreamOperator<OUT, O extends StreamOperator<OUT, O>> extends DataStream<OUT> {

	protected StreamOperator(StreamExecutionEnvironment context, TypeInformation<OUT> type) {
		super(context, type);
	}
}
