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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract channel state handle.
 *
 * @param <Info> type of channel info (e.g. {@link
 *     org.apache.flink.runtime.checkpoint.channel.InputChannelInfo InputChannelInfo}).
 */
@Internal
public abstract class AbstractChannelStateHandle<Info> implements StateObject {

    private static final long serialVersionUID = 1L;

    private final Info info;
    private final StreamStateHandle delegate;
    /**
     * Start offsets in a {@link org.apache.flink.core.fs.FSDataInputStream stream} {@link
     * StreamStateHandle#openInputStream obtained} from {@link #delegate}.
     */
    private final List<Long> offsets;

    private final long size;

    /** The original subtask index before rescaling recovery. */
    private final int subtaskIndex;

    AbstractChannelStateHandle(
            StreamStateHandle delegate,
            List<Long> offsets,
            int subtaskIndex,
            Info info,
            long size) {
        this.subtaskIndex = subtaskIndex;
        this.info = checkNotNull(info);
        this.delegate = checkNotNull(delegate);
        this.offsets = checkNotNull(offsets);
        this.size = size;
    }

    public static Set<StreamStateHandle> collectUniqueDelegates(
            Collection<? extends AbstractChannelStateHandle<?>>... collections) {
        Set<StreamStateHandle> result = new HashSet<>();
        for (Collection<? extends AbstractChannelStateHandle<?>> collection : collections) {
            for (AbstractChannelStateHandle<?> handle : collection) {
                result.add(handle.getDelegate());
            }
        }
        return result;
    }

    @Override
    public void discardState() throws Exception {
        delegate.discardState();
    }

    @Override
    public long getStateSize() {
        return size; // can not rely on delegate.getStateSize because it can be shared
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

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractChannelStateHandle<?> that = (AbstractChannelStateHandle<?>) o;
        return subtaskIndex == that.subtaskIndex
                && info.equals(that.info)
                && delegate.equals(that.delegate)
                && offsets.equals(that.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskIndex, info, delegate, offsets);
    }

    @Override
    public String toString() {
        return "AbstractChannelStateHandle{"
                + "info="
                + info
                + ", delegate="
                + delegate
                + ", offsets="
                + offsets
                + '}';
    }

    /** Describes the underlying content. */
    public static class StateContentMetaInfo {
        private final List<Long> offsets;
        private long size = 0;

        public StateContentMetaInfo() {
            this(new ArrayList<>(), 0);
        }

        public StateContentMetaInfo(List<Long> offsets, long size) {
            this.offsets = offsets;
            this.size = size;
        }

        public void withDataAdded(long offset, long size) {
            this.offsets.add(offset);
            this.size += size;
        }

        public List<Long> getOffsets() {
            return Collections.unmodifiableList(offsets);
        }

        public long getSize() {
            return size;
        }
    }
}
