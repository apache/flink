package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.metrics.Counter;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
/**
 * Created by jozh on 6/20/2017.
 */

public class EventhubProducerThread extends Thread {
	private final Logger logger;
	private final ProducerCache producerCache;
	private final Properties eventhubProps;
	private final EventHubClient producer;
	private volatile boolean running;
	private Counter commitSendCount;

	public EventhubProducerThread(
		Logger logger,
		String threadName,
		ProducerCache producerCache,
		Properties eventhubProps,
		Counter commitSendCount) throws IOException, ServiceBusException{

		super(threadName);
		setDaemon(true);

		this.logger = logger;
		this.producerCache = producerCache;
		this.eventhubProps = eventhubProps;
		this.commitSendCount = commitSendCount;

		ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(
			eventhubProps.getProperty("eventhubs.namespace"),
			eventhubProps.getProperty("eventhubs.name"),
			eventhubProps.getProperty("eventhubs.policyname"),
			eventhubProps.getProperty("eventhubs.policykey"));
		this.producer = EventHubClient.createFromConnectionStringSync(connectionStringBuilder.toString());
		this.running = true;
	}

	public void shutdown(){
		logger.info("Shutdown eventhub producer thread {} on demand", this.getName());
		running = false;
	}

	@Override
	public void run() {
		if (!running){
			logger.info("Eventhub producer thread is set to STOP, thread {} exit", this.getName());
			return;
		}

		try {
			logger.info("Eventhub producer thread {} started", this.getName());
			while (running){
				final ArrayList<EventData> events = producerCache.pollNextBatch();
				if (events != null && events.size() > 0){
					producer.sendSync(events);
					commitSendCount.inc(events.size());
					logger.info("Eventhub producer thread send {} events success", events.size());
				}
				else {
					logger.debug("Eventhub producer thread received a null eventdata from producer cache");
				}
			}
		}
		catch (Throwable t){
			logger.error("Sending events error, {}", t.toString());
			producerCache.reportError(t);
		}
		finally {
			logger.info("Exit from eventhub producer thread, {}", this.getName());
			if (producer != null){
				try {
					producer.closeSync();
				}
				catch (Exception ex) {
					logger.error("Close eventhubclient {} error {}", eventhubProps.getProperty("eventhubs.name"), ex.getMessage());
					producerCache.reportError(ex);
				}
			}
		}

		logger.info("EventhubProducerThread {} quit", this.getName());
	}
}
