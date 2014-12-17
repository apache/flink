package flink.graphs.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;

@SuppressWarnings("serial")
public class GraphUtils {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static DataSet<Integer> count(DataSet set, ExecutionEnvironment env) {
		List<Integer> list = new ArrayList<>();
		list.add(0);
		DataSet<Integer> initialCount = env.fromCollection(list);
        return set
                .map(new OneMapper())
                .union(initialCount)
                .reduce(new AddOnesReducer())
                .first(1);
    }

	private static final class OneMapper<T extends Tuple> implements MapFunction<T, Integer> {
            @Override
            public Integer map(T o) throws Exception {
                return 1;
            }
    }
    
    private static final class AddOnesReducer implements ReduceFunction<Integer> {
            @Override
            public Integer reduce(Integer one, Integer two) throws Exception {
                return one + two;
            }
    } 
}