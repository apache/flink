package org.apache.flink.table.plan.nodes.datastream.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;;

public class DataStreamProcTimeAggregateGlobalWindowFunction extends DataStreamProcTimeWindowAggregator
		implements AllWindowFunction<Row, Row, GlobalWindow> {

	public DataStreamProcTimeAggregateGlobalWindowFunction(List<String> aggregators, List<Integer> indexes,
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
	public void apply(GlobalWindow window, Iterable<Row> values, Collector<Row> out) throws Exception {

		Row lastElement = null;
		Row result = null;
		for (int i = 0; i < aggregators.size(); i++) {
			aggregatorImpl.get(i).reset();
		}

		for (Row rowObj : values) {
			lastElement = rowObj;

			for (int i = 0; i < aggregators.size(); i++) {
				aggregatorImpl.get(i).aggregate(lastElement.getField(indexes.get(i)));
			}
		}

		// conversion rules for windows typically insert the elements of the
		// object as well.
		// a verification for number of expected fields to be the sum of input
		// arity + number of aggregators can be done
		result = new Row(lastElement.getArity() + aggregators.size());
		for (int i = 0; lastElement != null && i < lastElement.getArity(); i++) {
			result.setField(i, lastElement.getField(i));
		}

		for (int i = 0; i < aggregators.size(); i++) {
			result.setField(lastElement.getArity() + i, aggregatorImpl.get(i).result());
			aggregatorImpl.get(i).reset();
		}

		out.collect(result);

	}

}
