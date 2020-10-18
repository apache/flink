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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.util.CollectionUtil;

import java.util.List;

/**
 * Runtime {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * Writer Writers} that have state.
 *
 * @param <InputT> The input type of the {@link Writer}.
 * @param <CommT> The committable type of the {@link Writer}.
 * @param <WriterStateT> The type of the {@link Writer Writer's} state.
 */
@Internal
final class StatefulWriterOperator<InputT, CommT, WriterStateT> extends AbstractWriterOperator<InputT, CommT> {

	/** The operator's state descriptor. */
	private static final ListStateDescriptor<byte[]> WRITER_RAW_STATES_DESC =
		new ListStateDescriptor<>("writer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

	/** Used to create the stateful {@link Writer}. */
	private final Sink<InputT, CommT, WriterStateT, ?> sink;

	/** The writer operator's state serializer. */
	private final SimpleVersionedSerializer<WriterStateT> writerStateSimpleVersionedSerializer;

	// ------------------------------- runtime fields ---------------------------------------

	/** The operator's state. */
	private ListState<WriterStateT> writerState;

	StatefulWriterOperator(
		final Sink<InputT, CommT, WriterStateT, ?> sink,
		final SimpleVersionedSerializer<WriterStateT> writerStateSimpleVersionedSerializer) {
		this.sink = sink;
		this.writerStateSimpleVersionedSerializer = writerStateSimpleVersionedSerializer;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		final ListState<byte[]> rawState = context.getOperatorStateStore().getListState(WRITER_RAW_STATES_DESC);
		writerState = new SimpleVersionedListState<>(rawState, writerStateSimpleVersionedSerializer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		writerState.update((List<WriterStateT>) writer.snapshotState());
	}

	@Override
	Writer<InputT, CommT, WriterStateT> createWriter() throws Exception {
		final List<WriterStateT> committables = CollectionUtil.iterableToList(writerState.get());
		return sink.createWriter(createInitContext(), committables);
	}
}
