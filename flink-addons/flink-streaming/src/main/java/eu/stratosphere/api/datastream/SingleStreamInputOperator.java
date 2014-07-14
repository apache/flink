package eu.stratosphere.api.datastream;

import eu.stratosphere.types.TypeInformation;

public abstract class SingleStreamInputOperator<IN, OUT, O extends SingleStreamInputOperator<IN, OUT, O>> extends StreamOperator<OUT, O> {

	protected SingleStreamInputOperator(StreamExecutionEnvironment context,
			TypeInformation<OUT> type) {
		super(context, type);
	}

}
