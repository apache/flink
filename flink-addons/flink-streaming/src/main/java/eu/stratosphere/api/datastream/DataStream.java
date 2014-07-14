package eu.stratosphere.api.datastream;

import eu.stratosphere.types.TypeInformation;

public class DataStream<T> {

	private final StreamExecutionEnvironment context;
	
	private final TypeInformation<T> type;	
	
	protected DataStream(StreamExecutionEnvironment context, TypeInformation<T> type) {
		if (context == null) {
			throw new NullPointerException("context is null");
		}

		if (type == null) {
			throw new NullPointerException("type is null");
		}
		
		this.context = context;
		this.type = type;
	}
	
	public TypeInformation<T> getType() {
		return this.type;
	}


}