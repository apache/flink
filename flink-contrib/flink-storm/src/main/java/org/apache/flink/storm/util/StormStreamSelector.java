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

package org.apache.flink.storm.util;

import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Used by {@link FlinkTopology} to split multiple declared output streams within Flink.
 */
public final class StormStreamSelector<T> implements OutputSelector<SplitStreamType<T>> {
	private static final long serialVersionUID = 2553423379715401023L;

	/** internal cache to avoid short living ArrayList objects. */
	private final HashMap<String, List<String>> streams = new HashMap<String, List<String>>();

	@Override
	public Iterable<String> select(SplitStreamType<T> value) {
		String sid = value.streamId;
		List<String> streamId = this.streams.get(sid);
		if (streamId == null) {
			streamId = new ArrayList<String>(1);
			streamId.add(sid);
			this.streams.put(sid, streamId);
		}
		return streamId;
	}

}
