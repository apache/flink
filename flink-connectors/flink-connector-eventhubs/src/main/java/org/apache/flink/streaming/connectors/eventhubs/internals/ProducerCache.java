/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.util.ExceptionUtils;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by jozh on 6/20/2017.
 */
public final class ProducerCache implements Closeable, Serializable {
	private static final Logger logger = LoggerFactory.getLogger(ProducerCache.class);
	private static final long defaultCheckQueueStatusInterval = 50;
	public static final int DEFAULTCAPACITY = 100;
	public static final long DEFAULTTIMEOUTMILLISECOND = 100;
	private final ArrayBlockingQueue<EventData> cacheQueue;
	private final int queueCapacity;
	private final long pollTimeout;
	private Date lastPollTime;
	private Throwable error;
	private volatile boolean closed;

	public ProducerCache(){
		this(DEFAULTCAPACITY, DEFAULTTIMEOUTMILLISECOND);
	}

	public ProducerCache(int capacity){
		this(capacity, DEFAULTTIMEOUTMILLISECOND);
	}

	public ProducerCache(int capacity, long timeout){
		this.queueCapacity = capacity;
		this.pollTimeout = timeout;
		this.cacheQueue = new ArrayBlockingQueue<EventData>(this.queueCapacity);
		this.lastPollTime = new Date();
		this.closed = false;
	}

	public void put(EventData value) throws Exception{
		if (value == null){
			logger.error("Received empty events from event producer");
			return;
		}

		synchronized (cacheQueue){
			while (cacheQueue.remainingCapacity() <= 0 && !closed){
				checkErr();
				logger.warn("Event queue is full, current size is {}", cacheQueue.size());
				cacheQueue.wait(defaultCheckQueueStatusInterval);
			}

			if (closed){
				logger.info("Cache is closed, event is dropped.");
				return;
			}

			cacheQueue.add(value);
			cacheQueue.notifyAll();

			logger.debug("Add event into queue");
		}
	}

	public ArrayList<EventData> pollNextBatch() throws InterruptedException{
		logger.debug("###Begin to poll all data from event cache queue");

		synchronized (cacheQueue){
			while (!isPollTimeout() && !closed && cacheQueue.remainingCapacity() > 0){
				cacheQueue.wait(defaultCheckQueueStatusInterval);
			}

			final ArrayList<EventData> result = new ArrayList<>(cacheQueue.size());
			for (EventData item : cacheQueue){
				result.add(item);
			}
			cacheQueue.clear();
			cacheQueue.notifyAll();

			lastPollTime = new Date();
			return result;
		}
	}

	public void reportError(Throwable t) {
		if (t == null){
			return;
		}

		synchronized (cacheQueue){
			if (error == null){
				error = t;
			}
			logger.info("Producer thread report a error： {}", t.toString());
		}
	}

	@Override
	public void close() {
		synchronized (cacheQueue){
			logger.info("Close cache on demand");
			closed = true;
			cacheQueue.notifyAll();
		}
	}

	public void checkErr() throws Exception {
		synchronized (cacheQueue){
			if (error != null){
				ExceptionUtils.rethrowException(error, error.getMessage());
			}
		}
	}

	private boolean isPollTimeout(){
		long pollInterval = (new Date()).getTime() - lastPollTime.getTime();
		return pollInterval > pollTimeout;
	}
}
