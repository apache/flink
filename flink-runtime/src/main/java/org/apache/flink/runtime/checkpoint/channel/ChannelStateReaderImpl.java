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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.RefCountingFSDataInputStream.RefCountingFSDataInputStreamFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link ChannelStateReader} implementation. Usage considerations:
 * <ol>
 *     <li>state of a channel can be read once per instance of this class; once done it returns
 *     {@link org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult#NO_MORE_DATA NO_MORE_DATA}</li>
 *     <li>reader/writer indices of the passed buffer are respected and updated</li>
 *     <li>buffers must be prepared (cleared) before passing to reader</li>
 *     <li>buffers must be released after use</li>
 * </ol>
 */
@NotThreadSafe
@Internal
public class ChannelStateReaderImpl implements ChannelStateReader {
	private static final Logger log = LoggerFactory.getLogger(ChannelStateReaderImpl.class);

	private final Map<InputChannelInfo, ChannelStateStreamReader> inputChannelHandleReaders;
	private final Map<ResultSubpartitionInfo, ChannelStateStreamReader> resultSubpartitionHandleReaders;
	private boolean isClosed = false;

	public ChannelStateReaderImpl(TaskStateSnapshot snapshot) {
		this(snapshot, new ChannelStateSerializerImpl());
	}

	ChannelStateReaderImpl(TaskStateSnapshot snapshot, ChannelStateSerializer serializer) {
		RefCountingFSDataInputStreamFactory streamFactory = new RefCountingFSDataInputStreamFactory(serializer);
		final HashMap<InputChannelInfo, ChannelStateStreamReader> inputChannelHandleReadersTmp = new HashMap<>();
		final HashMap<ResultSubpartitionInfo, ChannelStateStreamReader> resultSubpartitionHandleReadersTmp = new HashMap<>();
		for (Map.Entry<OperatorID, OperatorSubtaskState> e : snapshot.getSubtaskStateMappings()) {
			addReaders(inputChannelHandleReadersTmp, e.getValue().getInputChannelState(), streamFactory);
			addReaders(resultSubpartitionHandleReadersTmp, e.getValue().getResultSubpartitionState(), streamFactory);
		}
		inputChannelHandleReaders = inputChannelHandleReadersTmp; // memory barrier to allow another thread call clear()
		resultSubpartitionHandleReaders = resultSubpartitionHandleReadersTmp; // memory barrier to allow another thread call clear()
	}

	private <T> void addReaders(
			Map<T, ChannelStateStreamReader> readerMap,
			Collection<? extends AbstractChannelStateHandle<T>> handles,
			RefCountingFSDataInputStreamFactory streamFactory) {
		for (AbstractChannelStateHandle<T> handle : handles) {
			checkState(!readerMap.containsKey(handle.getInfo()), "multiple states exist for channel: " + handle.getInfo());
			readerMap.put(handle.getInfo(), new ChannelStateStreamReader(handle, streamFactory));
		}
	}

	@Override
	public boolean hasChannelStates() {
		return !(inputChannelHandleReaders.isEmpty() && resultSubpartitionHandleReaders.isEmpty());
	}

	@Override
	public ReadResult readInputData(InputChannelInfo info, Buffer buffer) throws IOException {
		Preconditions.checkState(!isClosed, "reader is closed");
		log.debug("readInputData, resultSubpartitionInfo: {} , buffer {}", info, buffer);
		ChannelStateStreamReader reader = inputChannelHandleReaders.get(info);
		return reader == null ? ReadResult.NO_MORE_DATA : reader.readInto(buffer);
	}

	@Override
	public ReadResult readOutputData(ResultSubpartitionInfo info, BufferBuilder bufferBuilder) throws IOException {
		Preconditions.checkState(!isClosed, "reader is closed");
		log.debug("readOutputData, resultSubpartitionInfo: {} , bufferBuilder {}", info, bufferBuilder);
		ChannelStateStreamReader reader = resultSubpartitionHandleReaders.get(info);
		return reader == null ? ReadResult.NO_MORE_DATA : reader.readInto(bufferBuilder);
	}

	@Override
	public void close() throws Exception {
		isClosed = true;
		try (Closer closer = Closer.create()) {
			for (Map<?, ChannelStateStreamReader> map : asList(inputChannelHandleReaders, resultSubpartitionHandleReaders)) {
				for (ChannelStateStreamReader reader : map.values()) {
					closer.register(reader);
				}
				map.clear();
			}
		}
	}

}
