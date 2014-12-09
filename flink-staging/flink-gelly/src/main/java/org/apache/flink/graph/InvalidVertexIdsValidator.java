package flink.graphs;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class InvalidVertexIdsValidator<K extends Comparable<K> & Serializable, VV extends Serializable,
        EV extends Serializable> extends  GraphValidator<K, VV, EV> {

    /**
     * Checks that the edge set input contains valid vertex Ids,
     * i.e. that they also exist in the vertex input set.
     * @return a singleton DataSet<Boolean> stating whether a graph is valid
     * with respect to its vertex ids.
     */
    @Override
    public DataSet<Boolean> validate(Graph<K, VV, EV> graph) throws Exception {
        DataSet<Tuple1<K>> edgeIds = graph.getEdges().flatMap(new MapEdgeIds<K, EV>()).distinct();
        DataSet<K> invalidIds = graph.getVertices().coGroup(edgeIds).where(0).equalTo(0)
                .with(new GroupInvalidIds<K, VV>()).first(1);

        return GraphUtils.count(invalidIds.map(new KToTupleMap()), ExecutionEnvironment.getExecutionEnvironment())
                .map(new InvalidIdsMap());
    }

    private static final class MapEdgeIds<K extends Comparable<K> & Serializable,
            EV extends Serializable> implements FlatMapFunction<Edge<K, EV>,
            Tuple1<K>> {

        @Override
        public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {
            out.collect(new Tuple1<K>(edge.f0));
            out.collect(new Tuple1<K>(edge.f1));
        }
    }

    private static final class GroupInvalidIds<K extends Comparable<K> & Serializable,
            VV extends Serializable> implements CoGroupFunction<Vertex<K, VV>, Tuple1<K>, K> {

        @Override
        public void coGroup(Iterable<Vertex<K, VV>> vertexId,
                            Iterable<Tuple1<K>> edgeId, Collector<K> out) {
            if (!(vertexId.iterator().hasNext())) {
                // found an id that doesn't exist in the vertex set
                out.collect(edgeId.iterator().next().f0);
            }
        }
    }

    private static final class KToTupleMap<K> implements MapFunction<K, Tuple1<K>> {

        @Override
        public Tuple1<K> map (K key)throws Exception {
            return new Tuple1<>(key);
        }
    }

    private static final class InvalidIdsMap implements MapFunction<Integer, Boolean> {

        @Override
        public Boolean map (Integer numberOfInvalidIds)throws Exception {
            return numberOfInvalidIds == 0;
        }
    }

}