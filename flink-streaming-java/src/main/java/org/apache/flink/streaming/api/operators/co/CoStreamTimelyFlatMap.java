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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.TimelyCoFlatMapFunction;
import org.apache.flink.streaming.api.operators.AbstractKeyedTwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

@Internal
public class CoStreamTimelyFlatMap<K, IN1, IN2, OUT>
		extends AbstractKeyedTwoInputStreamOperator<K, IN1, IN2, OUT>
		implements Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<K> keySerializer;

	private transient TimestampedCollector<OUT> collector;

	private transient TimerService timerService;

	private TimelyCoFlatMapFunction<IN1, IN2, OUT> flatMapFunction;

	public CoStreamTimelyFlatMap(
			TypeSerializer<K> keySerializer,
			KeySelector<IN1, K> keySelector1,
			KeySelector<IN2, K> keySelector2,
			TimelyCoFlatMapFunction<IN1, IN2, OUT> flatMapper) {
		super(flatMapper, keySerializer, keySelector1, keySelector2);

		this.flatMapFunction = flatMapper;
		this.keySerializer = keySerializer;
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
	public void processKeyedElement1(K key, StreamRecord<IN1> element) throws Exception {
		collector.setTimestamp(element);
		flatMapFunction.flatMap1(element.getValue(), timerService, collector);

	}

	@Override
	public void processKeyedElement2(K key, StreamRecord<IN2> element) throws Exception {
		collector.setTimestamp(element);
		flatMapFunction.flatMap2(element.getValue(), timerService, collector);
	}

	@Override
	public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		flatMapFunction.onTimer(timer.getTimestamp(), TimeDomain.EVENT_TIME, timerService, collector);
	}

	@Override
	public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		flatMapFunction.onTimer(timer.getTimestamp(), TimeDomain.PROCESSING_TIME, timerService, collector);
	}

	protected TimestampedCollector<OUT> getCollector() {
		return collector;
	}
}
