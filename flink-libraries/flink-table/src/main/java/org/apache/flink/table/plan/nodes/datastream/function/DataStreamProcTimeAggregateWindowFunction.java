package org.apache.flink.table.plan.nodes.datastream.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DataStreamProcTimeAggregateWindowFunction extends DataStreamProcTimeWindowAggregator 
	implements WindowFunction<Object, Object, Tuple, GlobalWindow> {

	public DataStreamProcTimeAggregateWindowFunction(List<String> aggregators, List<Integer> indexes,
			List<TypeInformation<?>> typeOutput, List<TypeInformation<?>> typeInput) {
		this.aggregators = aggregators;
		this.indexes = indexes;
		this.typeOutput = typeOutput;
		this.typeInput = typeInput;
		aggregatorImpl = new ArrayList<>();

		for (int i = 0; i < aggregators.size(); i++) {
			setAggregator(i, aggregators.get(i));
		}
	
	}

	@Override
	public void apply(Tuple key, GlobalWindow window, Iterable<Object> input, Collector<Object> out) throws Exception {
		

		Row lastElement=null;
		Row result=null;
		for (int i = 0; i < aggregators.size(); i++) {
			aggregatorImpl.get(i).reset();
		}
		
		for (Object rowObj : input) {
			lastElement = (Row) rowObj;
			
			for (int i = 0; i < aggregators.size(); i++) {				
				aggregatorImpl.get(i).aggregate(lastElement.getField(indexes.get(i)));
			}
		}

		//conversion rules for windows typically insert the elements of the object as well. 
		//a verification for number of expected fields to be the sum of input arity + number of aggregators can be done 		
		result = new Row(lastElement.getArity() + aggregators.size());
		for (int i = 0; lastElement!=null && i < lastElement.getArity(); i++) {
			result.setField(i, lastElement.getField(i));
		}
		
		for (int i = 0; i < aggregators.size(); i++) {
			result.setField(lastElement.getArity() + i, aggregatorImpl.get(i).result());
			aggregatorImpl.get(i).reset();
		}

		out.collect(result);

	}

}
