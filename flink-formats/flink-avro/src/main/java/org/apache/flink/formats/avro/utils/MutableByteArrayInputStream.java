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

package org.apache.flink.formats.avro.utils;

import java.io.ByteArrayInputStream;

/**
 * An extension of the ByteArrayInputStream that allows to change a buffer that should be read
 * without creating a new ByteArrayInputStream instance. This allows to re-use the same InputStream
 * instance, copying message to process, and creation of Decoder on every new message.
 */
public final class MutableByteArrayInputStream extends ByteArrayInputStream {

    public MutableByteArrayInputStream() {
        super(new byte[0]);
    }

    /**
     * Set buffer that can be read via the InputStream interface and reset the input stream. This
     * has the same effect as creating a new ByteArrayInputStream with a new buffer.
     *
     * @param buf the new buffer to read.
     */
    public void setBuffer(byte[] buf) {
        this.buf = buf;
        this.pos = 0;
        this.count = buf.length;
    }
}
