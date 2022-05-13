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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.util.Objects;

/**
 * An event that is used to (de)multiplex old channels over the same new channel.
 *
 * <p>During unaligned checkpoint recovery, if there is a rescaling, channels from the previous run
 * may not be available anymore for restoring the data. In that case, the data of several old
 * channels is sent over the same new channel through multiplexing. Each buffer is following this
 * {@code SubtaskConnectionDescriptor} such that the receiver can demultiplex them.
 */
public final class SubtaskConnectionDescriptor extends RuntimeEvent {

    private final int inputSubtaskIndex;
    private final int outputSubtaskIndex;

    public SubtaskConnectionDescriptor(int inputSubtaskIndex, int outputSubtaskIndex) {
        this.inputSubtaskIndex = inputSubtaskIndex;
        this.outputSubtaskIndex = outputSubtaskIndex;
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    @Override
    public void write(DataOutputView out) {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    public int getInputSubtaskIndex() {
        return inputSubtaskIndex;
    }

    public int getOutputSubtaskIndex() {
        return outputSubtaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SubtaskConnectionDescriptor that = (SubtaskConnectionDescriptor) o;
        return inputSubtaskIndex == that.inputSubtaskIndex
                && outputSubtaskIndex == that.outputSubtaskIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputSubtaskIndex, outputSubtaskIndex);
    }

    @Override
    public String toString() {
        return "SubtaskConnectionDescriptor{"
                + "inputSubtaskIndex="
                + inputSubtaskIndex
                + ", outputSubtaskIndex="
                + outputSubtaskIndex
                + '}';
    }
}
