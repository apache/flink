package org.apache.flink.api.java.aggregation;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction.ResultTypeBehavior;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

public class AggregationOperatorFactory {
	
	private static final AggregationOperatorFactory INSTANCE = new AggregationOperatorFactory();
	private ResultTypeFactory resultTypeFactory = new ResultTypeFactory();
	private ResultTupleFactory resultTupleFactory = new ResultTupleFactory();
	
	public <T, R extends Tuple> AggregationOperator<T, R> aggregate(DataSet<T> input, AggregationFunction<?, ?>[] functions) {
		TypeInformation<R> resultType = resultTypeFactory.createAggregationResultType(input.getType(), functions);
		int arity = resultType.getArity();
		R resultTuple = resultTupleFactory.createResultTuple(arity);
		AggregationOperator<T, R> op = new AggregationOperator<T, R>(input, resultType, resultTuple, functions);
		return op;
	}
	
	static class ResultTypeFactory {
	
		<R extends Tuple> TypeInformation<R> createAggregationResultType(TypeInformation<?> inputType, AggregationFunction<?, ?>... functions) {
			Validate.inclusiveBetween(1, Tuple.MAX_ARITY, functions.length, "Output tuple of aggregation must have between 1 and %s elements; requested tuple has %s elements.", Tuple.MAX_ARITY, functions.length);
			BasicTypeInfo<?>[] types = new BasicTypeInfo[functions.length];
			for (int i = 0; i < functions.length; ++i) {
				AggregationFunction<?, ?> function = functions[i];
				processAggregationFunction(inputType, types, i, function);
			}
			TypeInformation<R> resultType = new TupleTypeInfo<R>(types);
			return resultType;
		}

		private <T> void processAggregationFunction(TypeInformation<?> inputType,
				BasicTypeInfo<?>[] types, int i,
				AggregationFunction<T, ?> function) {
			ResultTypeBehavior resultTypeBehavior = function.getResultTypeBehavior();
			int fieldPosition = function.getFieldPosition();
			TupleTypeInfoBase<?> inputTypeAsTuple = (TupleTypeInfoBase<?>) inputType;
			@SuppressWarnings("unchecked")
			BasicTypeInfo<T> fieldType = (BasicTypeInfo<T>) inputTypeAsTuple.getTypeAt(fieldPosition);
			function.setInputType(fieldType);
			if (resultTypeBehavior == ResultTypeBehavior.FIXED) {
				types[i] = function.getResultType();
			} else if (resultTypeBehavior == ResultTypeBehavior.INPUT) {
				types[i] = fieldType;
			} else {
				throw new RuntimeException("Unknown aggregation function result type behavior: " + resultTypeBehavior);
			}
		}
	}
	
	static class ResultTupleFactory {
	
		@SuppressWarnings("unchecked")
		<R extends Tuple> R createResultTuple(int arity) {
			String resultTupleClassName = "org.apache.flink.api.java.tuple.Tuple" + String.valueOf(arity);
			Tuple result = null;
			try {
				result = (Tuple) Class.forName(resultTupleClassName).newInstance();
			} catch (InstantiationException e) {
				throw new IllegalArgumentException("Could not create output tuple", e);
			} catch (IllegalAccessException e) {
				throw new IllegalArgumentException("Could not create output tuple", e);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException("Could not create output tuple", e);
			}
			return (R) result;
		}
	}

	public static AggregationOperatorFactory getInstance() {
		return INSTANCE;
	}

	ResultTypeFactory getResultTypeFactory() {
		return resultTypeFactory;
	}

	void setResultTypeFactory(ResultTypeFactory resultTypeFactory) {
		this.resultTypeFactory = resultTypeFactory;
	}

	ResultTupleFactory getResultTupleFactory() {
		return resultTupleFactory;
	}

	void setResultTupleFactory(ResultTupleFactory resultTupleFactory) {
		this.resultTupleFactory = resultTupleFactory;
	}
	
}
