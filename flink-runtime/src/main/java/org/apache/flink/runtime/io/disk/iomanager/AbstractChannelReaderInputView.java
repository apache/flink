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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.AbstractPagedInputView;

import java.io.IOException;
import java.util.List;

/**
 * A {@link org.apache.flink.core.memory.DataInputView} that is backed by a {@link FileIOChannel},
 * making it effectively a data input stream. The view reads it data in blocks from the underlying
 * channel. The view can only read data that has been written by a {@link ChannelWriterOutputView},
 * due to block formatting.
 */
public abstract class AbstractChannelReaderInputView extends AbstractPagedInputView {

    public AbstractChannelReaderInputView(int headerLength) {
        super(headerLength);
    }

    /**
     * Closes this InputView, closing the underlying reader and returning all memory segments.
     *
     * @return A list containing all memory segments originally supplied to this view.
     * @throws IOException Thrown, if the underlying reader could not be properly closed.
     */
    public abstract List<MemorySegment> close() throws IOException;

    /** Get the underlying channel. */
    public abstract FileIOChannel getChannel();
}
