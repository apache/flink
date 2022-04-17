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

package org.apache.flink.changelog.fs;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

class ChangelogStreamWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogStreamWrapper.class);

    static DataInputViewStreamWrapper wrap(InputStream stream) throws IOException {
        BufferedInputStream bufferedStream = new BufferedInputStream(stream);
        boolean compressed = bufferedStream.read() == 1;
        return new DataInputViewStreamWrapper(
                compressed
                        ? SnappyStreamCompressionDecorator.INSTANCE.decorateWithCompression(
                                bufferedStream)
                        : bufferedStream) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    bufferedStream.close();
                }
            }
        };
    }

    static DataInputViewStreamWrapper wrapAndSeek(InputStream stream, long offset)
            throws IOException {
        DataInputViewStreamWrapper wrappedStream = wrap(stream);
        if (offset != 0) {
            LOG.debug("seek to {}", offset);
            wrappedStream.skipBytesToRead((int) offset);
        }
        return wrappedStream;
    }
}
