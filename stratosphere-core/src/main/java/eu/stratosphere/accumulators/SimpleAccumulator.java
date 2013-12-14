package eu.stratosphere.accumulators;




/**
 * Similar to Accumulator, but the type of items to add and the result value
 * must be the same.
 */
public interface SimpleAccumulator<T> extends Accumulator<T,T> {

}
