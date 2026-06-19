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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader.Decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A sink {@link Decoder} that reads data encoded by the {@link
 * org.apache.flink.api.common.serialization.SimpleStringEncoder} only for compaction. The original
 * input type is missing, but it's enough to read string contents for writing the compacted file.
 */
@PublicEvolving
public class SimpleStringDecoder implements Decoder<String> {

    private BufferedReader reader;

    @Override
    public void open(InputStream input) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(input));
    }

    @Override
    public String decodeNext() throws IOException {
        String nextLine = reader.readLine();
        // String read will be write directly to the compacted file, so the '\n' should be appended.
        return nextLine == null ? null : (nextLine + '\n');
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
