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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract channel state handle.
 * @param <Info> type of channel info (e.g. {@link org.apache.flink.runtime.checkpoint.channel.InputChannelInfo InputChannelInfo}).
 */
@Internal
public abstract class AbstractChannelStateHandle<Info> implements StateObject {

	private static final long serialVersionUID = 1L;

	private final Info info;
	private final StreamStateHandle delegate;
	/**
	 * Start offsets in a {@link org.apache.flink.core.fs.FSDataInputStream stream} {@link StreamStateHandle#openInputStream obtained} from {@link #delegate}.
	 */
	private final List<Long> offsets;

	AbstractChannelStateHandle(StreamStateHandle delegate, List<Long> offsets, Info info) {
		this.info = checkNotNull(info);
		this.delegate = checkNotNull(delegate);
		this.offsets = checkNotNull(offsets);
	}

	@Override
	public void discardState() throws Exception {
		delegate.discardState();
	}

	@Override
	public long getStateSize() {
		return delegate.getStateSize();
	}

	public List<Long> getOffsets() {
		return offsets;
	}

	public StreamStateHandle getDelegate() {
		return delegate;
	}

	public Info getInfo() {
		return info;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof AbstractChannelStateHandle)) {
			return false;
		}
		final AbstractChannelStateHandle<?> that = (AbstractChannelStateHandle<?>) o;
		return info.equals(that.info) && delegate.equals(that.delegate) && offsets.equals(that.offsets);
	}

	@Override
	public int hashCode() {
		return Objects.hash(info, delegate, offsets);
	}
}
