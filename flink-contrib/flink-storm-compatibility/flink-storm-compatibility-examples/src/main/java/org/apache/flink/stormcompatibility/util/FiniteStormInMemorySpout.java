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

package org.apache.flink.stormcompatibility.util;

import backtype.storm.tuple.Values;
import org.apache.flink.stormcompatibility.wrappers.FiniteStormSpout;

/**
 * Implements a Storm Spout that reads String[] data stored in the memory. The spout stops
 * automatically when it emitted all of the data.
 */
public class FiniteStormInMemorySpout extends AbstractStormSpout implements FiniteStormSpout {

	private static final long serialVersionUID = -4008858647468647019L;

	private String[] source;
	private int counter = 0;

	public FiniteStormInMemorySpout(String[] source) {
		this.source = source;
	}

	@Override
	public void nextTuple() {
			this.collector.emit(new Values(source[this.counter++]));
	}

	public boolean reachedEnd() {
		return counter >= source.length;
	}

}
