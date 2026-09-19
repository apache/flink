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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

/**
 * Marks the tail of recovered buffers that the spill drain pushed into a {@link
 * RecoverableInputChannel}. The consume path polls this sentinel to learn the exact moment all
 * recovered buffers have been consumed; it is never delivered to the operator. It is distinct from
 * {@link EndOfInputChannelStateEvent} (which terminates the {@link RecoveredInputChannel} read
 * stream) so the two recovery handoffs cannot be confused.
 */
public class EndOfFetchedChannelStateEvent extends RuntimeEvent {

    /** The singleton instance of this event. */
    public static final EndOfFetchedChannelStateEvent INSTANCE =
            new EndOfFetchedChannelStateEvent();

    // ------------------------------------------------------------------------

    // not instantiable
    private EndOfFetchedChannelStateEvent() {}

    // ------------------------------------------------------------------------

    @Override
    public void write(DataOutputView out) {
        throw new UnsupportedOperationException(
                "EndOfFetchedChannelStateEvent must be serialized via EventSerializer's dedicated"
                        + " type-tag path, not reflective write().");
    }

    @Override
    public void read(DataInputView in) {
        throw new UnsupportedOperationException(
                "EndOfFetchedChannelStateEvent must be deserialized via EventSerializer's dedicated"
                        + " type-tag path, not reflective read().");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return 20250814;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj.getClass() == EndOfFetchedChannelStateEvent.class;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
