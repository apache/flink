package org.apache.flink.streaming.api.environment;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.Collection;


public class PathAnalyzer {


    public static Integer computePathNum(StreamGraph streamGraph) throws  Exception{

        Collection<Integer> sourceNodeIds = streamGraph.getSourceIDs();
        if (sourceNodeIds.isEmpty()){
            throw new Exception("Stream graph contains no source");
        }

        if (sourceNodeIds.size() > 1){
            throw new Exception("Stream graph contains more than one source");
        }

        StreamNode curNode = streamGraph.getStreamNode((Integer) sourceNodeIds.toArray()[0]);
        int pathNum = 1;
        while (!curNode.getOutEdges().isEmpty()){
            if (curNode.getOutEdges().size() > 1){
                throw new Exception(String.format("Stream topology diverges at %s", curNode.getOperatorName()));
            }

            StreamEdge edge = curNode.getOutEdges().get(0);
            StreamPartitioner<?> partitioner =  edge.getPartitioner();
            StreamNode nextNode = streamGraph.getStreamNode(edge.getTargetId());


            if (partitioner instanceof ForwardPartitioner) {
                System.out.println("Forward partition. Path num does not change");
            } else if (partitioner instanceof BroadcastPartitioner ||
                    partitioner instanceof RebalancePartitioner ||
                    partitioner instanceof ShufflePartitioner ||
                    partitioner instanceof KeyGroupStreamPartitioner
            ) {
                System.out.println(partitioner.getClass().getName());

                pathNum *= nextNode.getParallelism();
            } else if (partitioner instanceof RescalePartitioner) {
                System.out.println("TODO: implement rescale partitioner path num computation");
            } else {
                throw new Exception(String.format("Unknown partitioner: %s", partitioner.getClass().getName()));
            }
            curNode = nextNode;
        }

        return pathNum;
    }
}
