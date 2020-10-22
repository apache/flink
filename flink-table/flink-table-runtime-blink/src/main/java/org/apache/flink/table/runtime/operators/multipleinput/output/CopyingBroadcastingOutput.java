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

package org.apache.flink.table.runtime.operators.multipleinput.output;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

/**
 * Special version of {@link BroadcastingOutput} that performs a shallow copy of the
 * {@link StreamRecord} to ensure that multi-output works correctly.
 *
 * <p>The functionality of this class is similar to {@link OperatorChain#CopyingBroadcastingOutputCollector}.
 */
public class CopyingBroadcastingOutput extends BroadcastingOutput {

	public CopyingBroadcastingOutput(Output<StreamRecord<RowData>>[] outputs) {
		super(outputs);
	}

	@Override
	public void collect(StreamRecord<RowData> record) {
		for (int i = 0; i < outputs.length - 1; i++) {
			StreamRecord<RowData> shallowCopy = record.copy(record.getValue());
			outputs[i].collect(shallowCopy);
		}

		if (outputs.length > 0) {
			// don't copy for the last output
			outputs[outputs.length - 1].collect(record);
		}
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		for (int i = 0; i < outputs.length - 1; i++) {
			StreamRecord<X> shallowCopy = record.copy(record.getValue());
			outputs[i].collect(outputTag, shallowCopy);
		}

		if (outputs.length > 0) {
			// don't copy for the last output
			outputs[outputs.length - 1].collect(outputTag, record);
		}
	}
}
