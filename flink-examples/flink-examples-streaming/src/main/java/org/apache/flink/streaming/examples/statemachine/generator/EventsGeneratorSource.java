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

package org.apache.flink.streaming.examples.statemachine.generator;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.examples.statemachine.event.Event;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A event stream source that generates the events on the fly. Useful for
 * self-contained demos.
 */
@SuppressWarnings("serial")
public class EventsGeneratorSource extends RichParallelSourceFunction<Event> {

	private final double errorProbability;

	private final int delayPerRecordMillis;

	private volatile boolean running = true;

	public EventsGeneratorSource(double errorProbability, int delayPerRecordMillis) {
		checkArgument(errorProbability >= 0.0 && errorProbability <= 1.0, "error probability must be in [0.0, 1.0]");
		checkArgument(delayPerRecordMillis >= 0, "delay must be >= 0");

		this.errorProbability = errorProbability;
		this.delayPerRecordMillis = delayPerRecordMillis;
	}

	@Override
	public void run(SourceContext<Event> sourceContext) throws Exception {
		final EventsGenerator generator = new EventsGenerator(errorProbability);

		final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
		final int min = range * getRuntimeContext().getIndexOfThisSubtask();
		final int max = min + range;

		while (running) {
			sourceContext.collect(generator.next(min, max));

			if (delayPerRecordMillis > 0) {
				Thread.sleep(delayPerRecordMillis);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
