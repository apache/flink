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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.dataformat.BaseRow;

/**
 * Base class for stream temporal sort operator.
 */
abstract class BaseTemporalSortOperator extends AbstractStreamOperator<BaseRow> implements
		OneInputStreamOperator<BaseRow, BaseRow>, Triggerable<BaseRow, VoidNamespace> {

	protected transient TimerService timerService;
	protected transient TimestampedCollector<BaseRow> collector;

	@Override
	public void open() throws Exception {
		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("user-timers",
				VoidNamespaceSerializer.INSTANCE,
				this);
		timerService = new SimpleTimerService(internalTimerService);
		collector = new TimestampedCollector<>(output);
	}

}
