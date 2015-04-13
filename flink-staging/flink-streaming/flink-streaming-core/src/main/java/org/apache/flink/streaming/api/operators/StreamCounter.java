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

package org.apache.flink.streaming.api.operators;


public class StreamCounter<IN> extends ChainableStreamOperator<IN, Long> {
	private static final long serialVersionUID = 1L;

	Long count = 0L;

	public StreamCounter() {
		super(null);
	}

	@Override
	public void run() throws Exception {
		while (isRunning && readNext() != null) {
			collector.collect(++count);
		}
	}

	@Override
	public void collect(IN record) {
		if (isRunning) {
			collector.collect(++count);
		}
	}

}
