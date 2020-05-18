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

import org.apache.flink.runtime.state.CheckpointStorageWorkerView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Maintains a set of {@link ChannelStateCheckpointWriter writers} per checkpoint and translates incoming
 * {@link ChannelStateWriteRequest requests} to their corresponding methods.
 */
final class ChannelStateWriteRequestDispatcherImpl implements ChannelStateWriteRequestDispatcher {
	private static final Logger LOG = LoggerFactory.getLogger(ChannelStateWriteRequestDispatcherImpl.class);

	private final Map<Long, ChannelStateCheckpointWriter> writers; // limited indirectly by results max size
	private final CheckpointStorageWorkerView streamFactoryResolver;
	private final ChannelStateSerializer serializer;

	ChannelStateWriteRequestDispatcherImpl(CheckpointStorageWorkerView streamFactoryResolver, ChannelStateSerializer serializer) {
		this.writers = new HashMap<>();
		this.streamFactoryResolver = checkNotNull(streamFactoryResolver);
		this.serializer = checkNotNull(serializer);
	}

	@Override
	public void dispatch(ChannelStateWriteRequest request) throws Exception {
		LOG.debug("process {}", request);
		try {
			dispatchInternal(request);
		} catch (Exception e) {
			try {
				request.cancel(e);
			} catch (Exception ex) {
				e.addSuppressed(ex);
			}
			throw e;
		}
	}

	private void dispatchInternal(ChannelStateWriteRequest request) throws Exception {
		if (request instanceof CheckpointStartRequest) {
			checkState(!writers.containsKey(request.getCheckpointId()), "writer not found for request " + request);
			writers.put(request.getCheckpointId(), buildWriter((CheckpointStartRequest) request));
		} else if (request instanceof CheckpointInProgressRequest) {
			ChannelStateCheckpointWriter writer = writers.get(request.getCheckpointId());
			CheckpointInProgressRequest req = (CheckpointInProgressRequest) request;
			if (writer == null) {
				req.onWriterMissing();
			} else {
				req.execute(writer);
			}
		} else {
			throw new IllegalArgumentException("unknown request type: " + request);
		}
	}

	private ChannelStateCheckpointWriter buildWriter(CheckpointStartRequest request) throws Exception {
		return new ChannelStateCheckpointWriter(
			request,
			streamFactoryResolver.resolveCheckpointStorageLocation(request.getCheckpointId(), request.getLocationReference()),
			serializer,
			() -> writers.remove(request.getCheckpointId()));
	}

	@Override
	public void fail(Throwable cause) {
		for (ChannelStateCheckpointWriter writer : writers.values()) {
			try {
				writer.fail(cause);
			} catch (Exception ex) {
				LOG.warn("unable to fail write channel state writer", cause);
			}
		}
		writers.clear();
	}
}
