package org.apache.flink.streaming.connectors.eventhubs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ExceptionUtils;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Created by jozh on 5/23/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 * Coming from flink kafka connector
 */
@ThreadSafe
public final class Handover implements Closeable {
	private static final int MAX_EVENTS_BLOCK_IN_QUEUE = 1000;
	private static final Logger logger = LoggerFactory.getLogger(Handover.class);
	private ConcurrentLinkedQueue<Tuple2<EventhubPartition, Iterable<EventData>>> eventQueue = new ConcurrentLinkedQueue();

	private volatile boolean allProducerWakeup = true;

	private Throwable error;

	public Tuple2<EventhubPartition, Iterable<EventData>> pollNext() throws Exception{
		logger.debug("###Begin to poll data from event cache queue");
		synchronized (eventQueue){
			while (eventQueue.isEmpty() && error == null){
				logger.debug("### No data in the msg queue, waiting... ");
				eventQueue.wait();
			}

			logger.debug("### Get notified from eventhub consummer thread");
			Tuple2<EventhubPartition, Iterable<EventData>> events = eventQueue.poll();
			if (events != null && events.f0 != null && events.f1 != null){
				logger.debug("### Get event data from {}", events.f0.toString());
				int queueSize = eventQueue.size();
				if (queueSize < MAX_EVENTS_BLOCK_IN_QUEUE / 2){
					eventQueue.notifyAll();
				}
				return events;
			}
			else {
				ExceptionUtils.rethrowException(error, error.getMessage());
				return null;
			}
		}
	}

	public void produce(final Tuple2<EventhubPartition, Iterable<EventData>> events) throws InterruptedException{
		if (events == null || events.f0 == null || events.f1 == null){
			logger.error("Received empty events from eventhub consumer thread");
			return;
		}

		synchronized (eventQueue){
			while (eventQueue.size() > MAX_EVENTS_BLOCK_IN_QUEUE){
				logger.warn("Event queue is full, current size is {}", eventQueue.size());
				eventQueue.wait();
			}

			eventQueue.add(events);
			eventQueue.notifyAll();
			logger.debug("Add received events into queue");
		}
	}

	@Override
	public void close() {
		synchronized (eventQueue){
			logger.info("Close handover on demand");
			eventQueue.clear();
			if (error == null){
				error = new Throwable("Handover closed on command");
			}

			eventQueue.notifyAll();
		}
	}

	public void reportError(Throwable t) {
		if (t == null){
			return;
		}

		synchronized (eventQueue){
			if (error == null){
				error = t;
			}
			eventQueue.clear();
			eventQueue.notifyAll();
			logger.info("Consumer thread report a error： {}", error.getMessage());
		}
	}

	public void wakeupProducer() {
		synchronized (eventQueue){
			logger.info("Wakeup producer on demand");
			eventQueue.clear();
			eventQueue.notifyAll();
		}
	}
}
