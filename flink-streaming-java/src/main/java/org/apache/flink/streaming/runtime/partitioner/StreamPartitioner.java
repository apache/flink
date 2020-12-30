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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Objects;

/** A special {@link ChannelSelector} for use in streaming programs. */
@Internal
public abstract class StreamPartitioner<T>
        implements ChannelSelector<SerializationDelegate<StreamRecord<T>>>, Serializable {
    private static final long serialVersionUID = 1L;

    protected int numberOfChannels;

    @Override
    public void setup(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public boolean isBroadcast() {
        return false;
    }

    public abstract StreamPartitioner<T> copy();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamPartitioner<?> that = (StreamPartitioner<?>) o;
        return numberOfChannels == that.numberOfChannels;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfChannels);
    }

    /**
     * Defines the behavior of this partitioner, when upstream rescaled during recovery of in-flight
     * data.
     */
    public SubtaskStateMapper getUpstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ARBITRARY;
    }

    /**
     * Defines the behavior of this partitioner, when downstream rescaled during recovery of
     * in-flight data.
     */
    public abstract SubtaskStateMapper getDownstreamSubtaskStateMapper();
}
