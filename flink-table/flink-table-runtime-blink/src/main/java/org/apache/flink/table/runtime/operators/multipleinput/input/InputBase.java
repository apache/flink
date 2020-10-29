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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.MultipleInputStreamOperatorBase;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base {@link Input} used in {@link MultipleInputStreamOperatorBase}.
 */
public abstract class InputBase implements Input<RowData> {

	@Nullable
	protected final KeySelector<?, ?> stateKeySelector;
	protected final AbstractStreamOperatorV2<RowData> owner;
	protected final int inputId;
	private final StreamOperator<RowData> operator;

	public InputBase(AbstractStreamOperatorV2<RowData> owner, int inputId, StreamOperator<RowData> operator) {
		checkArgument(inputId > 0, "indexId starts from 1");
		this.owner = checkNotNull(owner);
		this.inputId = inputId;
		this.stateKeySelector = owner.getOperatorConfig().getStatePartitioner(
				inputId - 1, owner.getUserCodeClassloader());
		this.operator = checkNotNull(operator);
	}

	@Override
	public void setKeyContextElement(StreamRecord record) throws Exception {
		internalSetKeyContextElement(record, stateKeySelector);
	}

	protected <T> void internalSetKeyContextElement(
			StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
		if (selector != null) {
			Object key = selector.getKey(record.getValue());
			operator.setCurrentKey(key);
		}
	}
}
