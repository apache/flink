/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.source;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomTupleSource implements SourceFunction<Tuple4<Integer, String, Double, Long>> {
	private final int count;
	private final Random random;
	private final long initialTimestamp;

	private volatile boolean isRunning = true;
	private volatile int number = 0;
	private long closeDelayTimestamp;

	public RandomTupleSource(int count, long initialTimestamp) {
		this.count = count;
		this.random = new Random();
		this.initialTimestamp = initialTimestamp;
	}

	public RandomTupleSource() {
		this(Integer.MAX_VALUE, System.currentTimeMillis());
	}

	public RandomTupleSource(int count) {
		this(count, System.currentTimeMillis());
	}


	public RandomTupleSource closeDelay(long delayTimestamp) {
		this.closeDelayTimestamp = delayTimestamp;
		return this;
	}

	@Override
	public void run(SourceContext<Tuple4<Integer, String, Double, Long>> ctx) throws Exception {
		while (isRunning) {
			ctx.collect(Tuple4.of(number, "test_tuple", random.nextDouble(), initialTimestamp + 1000 * number));
			number++;
			if (number >= this.count) {
				cancel();
			}
		}
	}

	@Override
	public void cancel() {
		this.isRunning = false;
		try {
			Thread.sleep(this.closeDelayTimestamp);
		} catch (InterruptedException e) {
			// ignored
		}
	}
}
