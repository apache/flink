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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TwoInputStreamOperator} for testing.
 */
public class TestingTwoInputStreamOperator extends AbstractStreamOperator<RowData>
		implements TwoInputStreamOperator<RowData, RowData, RowData>, BoundedMultiInput {

	private final List<Integer> endInputs = new ArrayList<>();
	private boolean isClosed = false;

	@Override
	public void processElement1(StreamRecord<RowData> element) throws Exception {

	}

	@Override
	public void processElement2(StreamRecord<RowData> element) throws Exception {

	}

	@Override
	public void endInput(int inputId) throws Exception {
		endInputs.add(inputId);
	}

	@Override
	public void close() throws Exception {
		isClosed = true;
	}

	public List<Integer> getEndInputs() {
		return endInputs;
	}

	public boolean isClosed() {
		return isClosed;
	}
}
