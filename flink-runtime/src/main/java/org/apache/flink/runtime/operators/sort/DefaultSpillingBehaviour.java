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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;

import java.io.IOException;

/**
 * A {@link SpillingThread.SpillingBehaviour} which spills or merges given elements directly.
 *
 * @see CombiningSpillingBehaviour
 */
final class DefaultSpillingBehaviour<R> implements SpillingThread.SpillingBehaviour<R> {

	private final boolean objectReuseEnabled;
	private final TypeSerializer<R> serializer;

	DefaultSpillingBehaviour(boolean objectReuseEnabled, TypeSerializer<R> serializer) {
		this.objectReuseEnabled = objectReuseEnabled;
		this.serializer = serializer;
	}

	@Override
	public void spillBuffer(
			CircularElement<R> element,
			ChannelWriterOutputView output,
			LargeRecordHandler<R> largeRecordHandler) throws IOException {
		element.getBuffer().writeToOutput(output, largeRecordHandler);
	}

	@Override
	public void mergeRecords(
		MergeIterator<R> mergeIterator,
		ChannelWriterOutputView output) throws IOException {
		// read the merged stream and write the data back
		if (objectReuseEnabled) {
			R rec = serializer.createInstance();
			while ((rec = mergeIterator.next(rec)) != null) {
				serializer.serialize(rec, output);
			}
		} else {
			R rec;
			while ((rec = mergeIterator.next()) != null) {
				serializer.serialize(rec, output);
			}
		}
	}
}
