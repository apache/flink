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

import org.apache.flink.runtime.memory.AbstractPagedOutputView;

import java.io.IOException;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link FileIOChannel},
 * making it effectively a data output stream. The view writes it data in blocks to the underlying
 * channel.
 */
public abstract class AbstractChannelWriterOutputView extends AbstractPagedOutputView {

    public AbstractChannelWriterOutputView(int segmentSize, int headerLength) {
        super(segmentSize, headerLength);
    }

    /** Get the underlying channel. */
    public abstract FileIOChannel getChannel();

    /**
     * Closes this OutputView, closing the underlying writer
     *
     * @return the number of bytes in last memory segment.
     */
    public abstract int close() throws IOException;

    /** Gets the number of blocks used by this view. */
    public abstract int getBlockCount();

    /** Get output bytes. */
    public abstract long getNumBytes() throws IOException;

    /** Get output compressed bytes, return num bytes if there is no compression. */
    public abstract long getNumCompressedBytes() throws IOException;
}
