package eu.stratosphere.api.datastream;

import java.util.Random;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

public class DataStream<T extends Tuple> {

	private final StreamExecutionEnvironment context;
	private TypeInformation<T> type;
	private final Random random = new Random();
	private final String id;
	
	protected DataStream() {
		// TODO implement
		context = new StreamExecutionEnvironment();
		id = "source";
		type = null;
	}
	
	protected DataStream(StreamExecutionEnvironment context, TypeInformation<T> type) {
		if (context == null) {
			throw new NullPointerException("context is null");
		}

		if (type == null) {
			throw new NullPointerException("type is null");
		}
		
		this.id = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
		this.context = context;
		this.type = type;
	}
	
	public String getId() {
		return id;
	}

	public <R extends Tuple> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapper) {
		TypeInformation<R> returnType = TypeExtractor.getFlatMapReturnTypes(flatMapper, type);
		return context.addFlatMapFunction(this, flatMapper, returnType);
	}
	
//	public <R> DataStream<R> map(MapFunction<T, R> mapper) {
//		TypeInformation<R> returnType = TypeExtractor.getMapReturnTypes(mapper, type);
//		return context.addMapFunction(this, mapper, returnType);
//	}
	
	protected void setType(TypeInformation<T> type) {
		this.type = type;
	}
	
	public TypeInformation<T> getType() {
		return this.type;
	}
}