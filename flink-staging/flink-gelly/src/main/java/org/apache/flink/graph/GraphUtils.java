package flink.graphs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;

@SuppressWarnings("serial")
public class GraphUtils {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static DataSet<Integer> count(DataSet set) {
        return set
                .map(new OneMapper())
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
