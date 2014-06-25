package eu.stratosphere.api.common.functions;

import eu.stratosphere.util.Collector;

import java.util.Iterator;

public interface GenericMapPartition<T, O> extends Function {

    /**
     * A user-implemented function that modifies or transforms an incoming object.
     *
     * @param records All records for the mapper
     * @param out The collector to hand results to.
     * @throws Exception
     */
    void mapPartition(Iterator<T> records, Collector<O> out) throws Exception;
}