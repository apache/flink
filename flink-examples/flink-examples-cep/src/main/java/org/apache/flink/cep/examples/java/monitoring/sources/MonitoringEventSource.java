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

package org.apache.flink.cep.examples.java.monitoring.sources;

import org.apache.flink.cep.examples.java.monitoring.events.MonitoringEvent;
import org.apache.flink.cep.examples.java.monitoring.events.PowerEvent;
import org.apache.flink.cep.examples.java.monitoring.events.TemperatureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Event source that randomly produces {@link TemperatureEvent} and {@link PowerEvent}.
 * The ratio of temperature events is configured by {@link tempEventRatio}.
 * The {@link TemperatureEvent#temperature} is a Gaussian distributed random number
 * with mean {@link TEMP_MEAN} and standard deviation {@link TEMP_STD}.
 * {@link PowerEvent#voltage} is generated in a similar way.
 */
public class MonitoringEventSource extends RichParallelSourceFunction<MonitoringEvent> {

	private int numRacks;
	private double tempEventRatio;

	public MonitoringEventSource() {
		this(DEFAULT_NUM_RACKS, DEFAULT_TEMP_EVENT_RATIO);
	}

	public MonitoringEventSource(int numRacks, double tempEventRatio) {
		this.numRacks = numRacks;
		this.tempEventRatio = tempEventRatio;
	}

	private static final int DEFAULT_NUM_RACKS = 10;
	private static final double DEFAULT_TEMP_EVENT_RATIO = 0.7;

	private static final double TEMP_STD = 20;
	private static final double TEMP_MEAN = 80;
	private static final double POWER_STD = 10;
	private static final double POWER_MEAN = 100;

	private boolean running = true;

	private Random random;

	// the number of racks for which to emit monitoring events from this sub source task
	private int shards;
	// rack id of the first shard
	private int offset;

	@Override
	public void open(Configuration configuration) {
		int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		int index = getRuntimeContext().getIndexOfThisSubtask();

		offset = (int) ((double) numRacks / numberTasks * index);
		shards = (int) ((double) numRacks / numberTasks * (index + 1)) - offset;

		random = new Random();
	}

	public void run(SourceContext<MonitoringEvent> sourceContext) throws Exception {
		while (running) {
			MonitoringEvent monitoringEvent;

			int rackId = offset + random.nextInt(shards);

			if (random.nextDouble() >= tempEventRatio) {
				double power = random.nextGaussian() * POWER_STD + POWER_MEAN;
				monitoringEvent = new PowerEvent(rackId, power);
			} else {
				double temperature = random.nextGaussian() * TEMP_STD + TEMP_MEAN;
				monitoringEvent = new TemperatureEvent(rackId, temperature);
			}

			sourceContext.collect(monitoringEvent);

			Thread.sleep(100);
		}
	}

	public void cancel() {
		running = false;
	}

}
