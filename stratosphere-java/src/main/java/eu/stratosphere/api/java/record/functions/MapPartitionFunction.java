package eu.stratosphere.api.java.record.functions;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMapPartition;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

/**
 * The MapPartitionFunction must be extended to provide a map partition implementation
 * By definition, the map partition is called for a full input set.
 */
public abstract class MapPartitionFunction extends AbstractFunction implements GenericMapPartition<Record, Record> {

	private static final long serialVersionUID = 1L;

	/**
	 * This method must be implemented to provide a user implementation of a mappartitioner.
	 * It is called for a full input set.
	 *
	 * @param records all input records
	 * @param out A collector that collects all output records.
	 *
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the map task and lets the fail-over logic
	 *                   decide whether to retry the mapper execution.
	 */
	@Override
	public abstract void mapPartition(Iterator<Record> records, Collector<Record> out) throws Exception;
}
