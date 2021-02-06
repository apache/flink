package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.RepartitionTaskEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class DynamicFixedPartitioner<T> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	protected static final Logger LOG = LoggerFactory.getLogger(DynamicFixedPartitioner.class);

	//	private int nextChannelToSendTo;
	private int nextChannelToSendTo = 0;

	private int maxParallelism = 0;

	protected final Random rng = new XORShiftRandom();

	@Override
	public StreamPartitioner<T> copy() { return this; }

	/**
	 * Returns the logical channel index, to which the given record should be written. It is
	 * illegal to call this method for broadcast channel selectors and this method can remain
	 * not implemented in that case (for example by throwing {@link UnsupportedOperationException}).
	 *
	 * @param record the record to determine the output channels for.
	 * @return an integer number which indicates the index of the output
	 * channel through which the record shall be forwarded.
	 */
	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		return nextChannelToSendTo;
	}

	/**
	 * Configure the {@link StreamPartitioner} with the maximum parallelism of the down stream
	 * operator.
	 *
	 * @param maxParallelism Maximum parallelism of the down stream operator.
	 */
	@Override
	public void configure(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	@Override
	public void handleEvent(AbstractEvent event) {
		nextChannelToSendTo = ((RepartitionTaskEvent)event).getInteger();
		LOG.debug("DynamicFixedPartitioner handleEvent RepartitionTaskEvent {}", nextChannelToSendTo);
	}

	@Override
	public String toString() {
		return "DYNAMICFIXED";
	}
}
