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

package org.apache.flink.storm.util;

import org.apache.storm.tuple.Values;

/**
 * Implements a Spout that reads data stored in memory.
 */
public class InMemorySpout<T> extends AbstractLineSpout {
	private static final long serialVersionUID = -4008858647468647019L;

	protected T[] source;
	protected int counter = 0;

	public InMemorySpout(T[] source) {
		this.source = source;
	}

	@Override
	public void nextTuple() {
		if (this.counter < source.length) {
			this.collector.emit(new Values(source[this.counter++]));
		}
	}

}
