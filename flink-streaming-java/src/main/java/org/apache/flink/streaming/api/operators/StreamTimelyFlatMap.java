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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.TimelyFlatMapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

@Internal
public class StreamTimelyFlatMap<K, IN, OUT>
		extends AbstractUdfStreamOperator<OUT, TimelyFlatMapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<K> keySerializer;

	private transient TimestampedCollector<OUT> collector;

	private transient TimerService timerService;

	public StreamTimelyFlatMap(TypeSerializer<K> keySerializer, TimelyFlatMapFunction<IN, OUT> flatMapper) {
		super(flatMapper);

		this.keySerializer = keySerializer;

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		InternalTimerService<VoidNamespace> internalTimerService =
				getInternalTimerService("user-timers", keySerializer, VoidNamespaceSerializer.INSTANCE, this);

		this.timerService = new SimpleTimerService(internalTimerService);
	}

	@Override
	public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		userFunction.onTimer(timer.getTimestamp(), TimeDomain.EVENT_TIME, timerService, collector);
	}

	@Override
	public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		userFunction.onTimer(timer.getTimestamp(), TimeDomain.PROCESSING_TIME, timerService, collector);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		userFunction.flatMap(element.getValue(), timerService, collector);
	}
}
