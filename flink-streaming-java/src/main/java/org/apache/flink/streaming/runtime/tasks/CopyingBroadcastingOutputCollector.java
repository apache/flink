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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;

/**
 * Special version of {@link BroadcastingOutputCollector} that performs a shallow copy of the
 * {@link StreamRecord} to ensure that multi-chaining works correctly.
 */
final class CopyingBroadcastingOutputCollector<T> extends BroadcastingOutputCollector<T> {

	public CopyingBroadcastingOutputCollector(
			Output<StreamRecord<T>>[] allOutputs,
			StreamStatusProvider streamStatusProvider,
			Counter numRecordsOutForTask) {
		super(allOutputs, streamStatusProvider, numRecordsOutForTask);
	}

	@Override
	public void collect(StreamRecord<T> record) {
		boolean emitted = false;
		for (int i = 0; i < nonChainedOutputs.length - 1; i++) {
			RecordWriterOutput<T> output = nonChainedOutputs[i];
			StreamRecord<T> shallowCopy = record.copy(record.getValue());
			emitted |= output.collectAndCheckIfEmitted(shallowCopy);
		}

		if (chainedOutputs.length == 0 && nonChainedOutputs.length > 0) {
			emitted |= nonChainedOutputs[nonChainedOutputs.length - 1].collectAndCheckIfEmitted(record);
		} else if (nonChainedOutputs.length > 0) {
			StreamRecord<T> shallowCopy = record.copy(record.getValue());
			emitted |= nonChainedOutputs[nonChainedOutputs.length - 1].collectAndCheckIfEmitted(shallowCopy);
		}

		if (chainedOutputs.length > 0){
			for (int i = 0; i < chainedOutputs.length - 1; i++) {
				Output<StreamRecord<T>> output = chainedOutputs[i];
				StreamRecord<T> shallowCopy = record.copy(record.getValue());
				output.collect(shallowCopy);
			}
			chainedOutputs[chainedOutputs.length - 1].collect(record);
		}

		if (emitted) {
			numRecordsOutForTask.inc();
		}
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		boolean emitted = false;
		for (int i = 0; i < nonChainedOutputs.length - 1; i++) {
			RecordWriterOutput<T> output = nonChainedOutputs[i];
			StreamRecord<X> shallowCopy = record.copy(record.getValue());
			emitted |= output.collectAndCheckIfEmitted(outputTag, shallowCopy);
		}

		if (chainedOutputs.length == 0 && nonChainedOutputs.length > 0) {
			emitted |= nonChainedOutputs[nonChainedOutputs.length - 1].collectAndCheckIfEmitted(outputTag, record);
		} else if (nonChainedOutputs.length > 0) {
			StreamRecord<X> shallowCopy = record.copy(record.getValue());
			emitted |= nonChainedOutputs[nonChainedOutputs.length - 1].collectAndCheckIfEmitted(outputTag, shallowCopy);
		}

		if (chainedOutputs.length > 0){
			for (int i = 0; i < chainedOutputs.length - 1; i++) {
				Output<StreamRecord<T>> output = chainedOutputs[i];
				StreamRecord<X> shallowCopy = record.copy(record.getValue());
				output.collect(outputTag, shallowCopy);
			}
			chainedOutputs[chainedOutputs.length - 1].collect(outputTag, record);
		}

		if (emitted) {
			numRecordsOutForTask.inc();
		}
	}
}
