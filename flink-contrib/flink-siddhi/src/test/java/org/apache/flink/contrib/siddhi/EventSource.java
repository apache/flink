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

package org.apache.flink.contrib.siddhi;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class EventSource implements SourceFunction<Event> {
	private final int count;
	private final Random random;

	private volatile boolean isRunning  = true;
	private volatile int number = 0;

	public EventSource(int count){
		this.count = count;
		this.random = new Random();
	}

	public EventSource(){
		this(Integer.MAX_VALUE);
	}

	@Override
	public void run(SourceContext<Event> ctx) throws Exception {
		while (isRunning){
			Thread.sleep(500);
			ctx.collect(Event.of(number, "test_event", random.nextDouble()));
			number ++;
			if(number>=this.count)
				cancel();
		}
	}

	@Override
	public void cancel() {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// ignored
		}
		this.isRunning = false;
	}
}
