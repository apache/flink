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

package org.apache.flink.streaming.examples.relational;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperator} for processing with priority.
 * {@link CoFlatMapFunction CoFlatMapFunctions}.
 */
@Internal
public class PriorityCoStreamFlatMap<IN1, IN2, OUT> extends CoStreamFlatMap<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	private transient boolean firstFinished;

	private transient boolean secondFinished;

	public PriorityCoStreamFlatMap(CoFlatMapFunction<IN1, IN2, OUT> flatMapper) {
		super(flatMapper);
	}

	@Override
	public void open() throws Exception {
		super.open();
		firstFinished = false;
		secondFinished = false;
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		Preconditions.checkState(!firstFinished);
		return TwoInputSelection.FIRST;
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<IN1> element) throws Exception {
		Preconditions.checkState(!firstFinished);
		super.processElement1(element);
		return TwoInputSelection.FIRST;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<IN2> element) throws Exception {
		Preconditions.checkState(firstFinished);
		Preconditions.checkState(!secondFinished);
		super.processElement2(element);
		return TwoInputSelection.SECOND;
	}

	@Override
	public void endInput1() {
		Preconditions.checkState(!firstFinished);
		firstFinished = true;
	}

	@Override
	public void endInput2() {
		Preconditions.checkState(firstFinished);
		Preconditions.checkState(!secondFinished);
		secondFinished = true;
	}
}
