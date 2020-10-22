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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.table.data.RowData;

/**
 * A special operator which collects all inputs' records and forwards them
 * in {@link MultipleInputStreamOperatorBase}.
 */
public class UnionStreamOperator extends StreamMap<RowData, RowData> implements BoundedMultiInput {
	private static final long serialVersionUID = 1L;

	public UnionStreamOperator() {
		// use MapFunction to combine the input data
		super((MapFunction<RowData, RowData>) value -> value);
	}

	@Override
	public void endInput(int inputId) throws Exception {
		// do nothing
	}
}
