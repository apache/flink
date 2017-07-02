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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;

/**
 * Strips {@link SplitStreamType}{@code <T>} away, ie, extracts the wrapped record of type {@code T}. Can be used to get
 * a "clean" stream from a Spout/Bolt that declared multiple output streams (after the streams got separated using
 * {@link DataStream#split(org.apache.flink.streaming.api.collector.selector.OutputSelector) .split(...)} and
 * {@link SplitStream#select(String...) .select(...)}).
 *
 * @param <T>
 */
public class SplitStreamMapper<T> implements MapFunction<SplitStreamType<T>, T> {
	private static final long serialVersionUID = 3550359150160908564L;

	@Override
	public T map(SplitStreamType<T> value) throws Exception {
		return value.value;
	}

}
