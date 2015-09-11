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
package org.apache.flink.stormcompatibility.api;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.stormcompatibility.util.SplitStreamType;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.streaming.util.keys.KeySelectorUtil.ArrayKeySelector;

/**
 * {@link SplitStreamTypeKeySelector} is a specific grouping key selector for streams that are selected via
 * {@link FlinkStormStreamSelector} from a Spout or Bolt that declares multiple output streams.
 * 
 * It extracts the wrapped {@link Tuple} type from the {@link SplitStreamType} tuples and applies a regular
 * {@link ArrayKeySelector} on it.
 */
public class SplitStreamTypeKeySelector implements KeySelector<SplitStreamType<Tuple>, Tuple> {
	private static final long serialVersionUID = 4672434660037669254L;

	private final ArrayKeySelector<Tuple> selector;

	public SplitStreamTypeKeySelector(int... fields) {
		this.selector = new KeySelectorUtil.ArrayKeySelector<Tuple>(fields);
	}

	@Override
	public Tuple getKey(SplitStreamType<Tuple> value) throws Exception {
		return selector.getKey(value.value);
	}

}
