package eu.stratosphere.streaming.api.streamcomponent;

import java.util.concurrent.BlockingQueue;

import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class BlockingQueueBroker extends Broker<BlockingQueue<StreamRecord>>{
	/**
	 * Singleton instance
	 */
	private static final BlockingQueueBroker INSTANCE = new BlockingQueueBroker();

	private BlockingQueueBroker() {}

	/**
	 * retrieve singleton instance
	 */
	public static Broker<BlockingQueue<StreamRecord>> instance() {
		return INSTANCE;
	}
}
