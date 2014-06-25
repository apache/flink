package eu.stratosphere.api.java.functions;


import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMapPartition;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

public abstract class MapPartitionFunction<IN, OUT> extends AbstractFunction implements GenericMapPartition<IN, OUT> {

    private static final long serialVersionUID = 1L;
    /**
     *
     * @param records All records for the mapper
     * @param out The collector to hand results to.
     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    @Override
    public abstract void mapPartition(Iterator<IN> records, Collector<OUT> out) throws Exception;
}
