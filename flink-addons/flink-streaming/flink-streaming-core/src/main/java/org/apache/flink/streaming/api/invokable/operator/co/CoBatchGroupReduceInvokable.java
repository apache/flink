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

package org.apache.flink.streaming.api.invokable.operator.co;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;

public class CoBatchGroupReduceInvokable<IN1, IN2, OUT> extends CoGroupReduceInvokable<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;
	protected long startCounter1;
	protected long startCounter2;
	protected long endCounter1;
	protected long endCounter2;

	public CoBatchGroupReduceInvokable(CoGroupReduceFunction<IN1, IN2, OUT> reduceFunction,
			long windowSize1, long windowSize2, long slideInterval1, long slideInterval2) {
		super(reduceFunction, windowSize1, windowSize2, slideInterval1, slideInterval2);
	}

	@Override
	protected void handleStream1() throws Exception {
		circularList1.add(reuse1);
		if (windowStart1()) {
			circularList1.newSlide();
		}
		if (windowEnd1()) {
			reduce1();
			circularList1.shiftWindow();
		}
	}

	@Override
	protected void handleStream2() throws Exception {
		circularList2.add(reuse2);
		if (windowStart2()) {
			circularList2.newSlide();
		}
		if (windowEnd2()) {
			reduce2();
			circularList2.shiftWindow();
		}
	}

	@Override
	protected boolean windowStart1() throws Exception {
		if (startCounter1 == slideInterval1) {
			startCounter1 = 0;
			return true;
		}
		return false;
	}

	@Override
	protected boolean windowStart2() throws Exception {
		if (startCounter2 == slideInterval2) {
			startCounter2 = 0;
			return true;
		}
		return false;
	}

	@Override
	protected boolean windowEnd1() throws Exception {
		if (endCounter1 == windowSize1) {
			endCounter1 -= slideInterval1;
			return true;
		}
		return false;
	}

	@Override
	protected boolean windowEnd2() throws Exception {
		if (endCounter2 == windowSize2) {
			endCounter2 -= slideInterval2;
			return true;
		}
		return false;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		startCounter1 = 0;
		startCounter2 = 0;
		endCounter1 = 0;
		endCounter2 = 0;
	}

	@Override
	protected void initialize1() {
		startCounter1++;
		endCounter1++;
	}

	@Override
	protected void initialize2() {
		startCounter2++;
		endCounter2++;
	}

}
