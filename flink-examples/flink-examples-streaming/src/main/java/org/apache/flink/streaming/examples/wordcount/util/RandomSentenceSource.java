/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcount.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * A SourceFunction that generates random data continuously with a configurable interval.
 */
public class RandomSentenceSource implements SourceFunction<String> {

	private volatile boolean isRunning = true;

	private final long intervalMs;

	private final Random rand = new Random();

	public RandomSentenceSource(long intervalMs) {
		if (intervalMs < 0) {
			System.out.println("intervalMs for RandomSource can't be negative; setting it to 0");
			this.intervalMs = 0;
		} else {
			this.intervalMs = intervalMs;
		}
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (isRunning) {
			int index = rand.nextInt(WordCountData.WORDS.length);
			ctx.collect(WordCountData.WORDS[index]);

			//wait for intervalMs before sending the next data
			Thread.sleep(intervalMs);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
