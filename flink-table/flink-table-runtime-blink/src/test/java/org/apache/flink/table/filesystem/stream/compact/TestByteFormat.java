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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;

/** Test byte stream format. */
@SuppressWarnings("serial")
public class TestByteFormat extends SimpleStreamFormat<Byte> {

    @Override
    public Reader<Byte> createReader(Configuration config, FSDataInputStream stream) {
        return new Reader<Byte>() {
            @Override
            public Byte read() throws IOException {
                byte b = (byte) stream.read();
                if (b == -1) {
                    return null;
                }
                return b;
            }

            @Override
            public void close() throws IOException {
                stream.close();
            }
        };
    }

    @Override
    public TypeInformation<Byte> getProducedType() {
        return BasicTypeInfo.BYTE_TYPE_INFO;
    }

    public static BulkFormat<Byte, FileSourceSplit> bulkFormat() {
        return new StreamFormatAdapter<>(new TestByteFormat());
    }
}
