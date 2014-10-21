package flink.graphs;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;

public class GraphUtils {

    public static DataSet<Integer> count (DataSet<Object> set) {
        return set
                .map(new MapFunction<Object, Integer>() {
                    @Override
                    public Integer map(Object o) throws Exception {
                        return 1;
                    }
                })
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer one, Integer two) throws Exception {
                        return one + two;
                    }
                })
                .first(1);
    }

}
