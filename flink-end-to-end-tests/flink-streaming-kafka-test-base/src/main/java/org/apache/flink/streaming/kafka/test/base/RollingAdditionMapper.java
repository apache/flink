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

package org.apache.flink.streaming.kafka.test.base;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
 * The current total count is keyed state managed by Flink.
 */
public class RollingAdditionMapper extends RichMapFunction<KafkaEvent, KafkaEvent> {

	private static final long serialVersionUID = 1180234853172462378L;

	private transient ValueState<Integer> currentTotalCount;

	@Override
	public KafkaEvent map(KafkaEvent event) throws Exception {
		Integer totalCount = currentTotalCount.value();

		if (totalCount == null) {
			totalCount = 0;
		}
		totalCount += event.getFrequency();

		currentTotalCount.update(totalCount);

		return new KafkaEvent(event.getWord(), totalCount, event.getTimestamp());
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
	}
}
