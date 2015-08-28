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

import org.apache.flink.stormcompatibility.wrappers.FiniteStormSpout;

/**
 * Implements a Storm Spout that reads String[] data stored in the memory. The spout stops
 * automatically when it emitted all of the data.
 */
public class FiniteStormInMemorySpout extends StormInMemorySpout<String> implements
		FiniteStormSpout {
	private static final long serialVersionUID = -4008858647468647019L;

	public FiniteStormInMemorySpout(String[] source) {
		super(source);
	}

	@Override
	public boolean reachedEnd() {
		return counter >= source.length;
	}

}
