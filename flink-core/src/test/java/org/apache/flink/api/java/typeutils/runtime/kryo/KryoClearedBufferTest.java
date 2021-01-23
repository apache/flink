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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class KryoClearedBufferTest {

    /**
     * Tests that the kryo output buffer is cleared in case of an exception. Flink uses the
     * EOFException to signal that a buffer is full. In such a case, the record which was tried to
     * be written will be rewritten. Therefore, eventually buffered data of this record has to be
     * cleared.
     */
    @Test
    public void testOutputBufferedBeingClearedInCaseOfException() throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.registerTypeWithKryoSerializer(
                TestRecord.class, new TestRecordSerializer());
        executionConfig.registerKryoType(TestRecord.class);

        KryoSerializer<TestRecord> kryoSerializer =
                new KryoSerializer<TestRecord>(TestRecord.class, executionConfig);

        int size = 94;
        int bufferSize = 150;

        TestRecord testRecord = new TestRecord(size);

        TestDataOutputView target = new TestDataOutputView(bufferSize);

        kryoSerializer.serialize(testRecord, target);

        try {
            kryoSerializer.serialize(testRecord, target);
            Assert.fail("Expected an EOFException.");
        } catch (EOFException eofException) {
            // expected exception
            // now the Kryo Output should have been cleared
        }

        TestRecord actualRecord =
                kryoSerializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(target.getBuffer())));

        Assert.assertEquals(testRecord, actualRecord);

        target.clear();

        // if the kryo output has been cleared then we can serialize our test record into the target
        // because the target buffer 150 bytes can host one TestRecord (total serialization size
        // 100)
        kryoSerializer.serialize(testRecord, target);

        byte[] buffer = target.getBuffer();
        int counter = 0;

        for (int i = 0; i < buffer.length; i++) {
            if (buffer[i] == 42) {
                counter++;
            }
        }

        Assert.assertEquals(size, counter);
    }

    public static class TestRecord {
        private byte[] buffer;

        public TestRecord(int size) {
            buffer = new byte[size];

            Arrays.fill(buffer, (byte) 42);
        }

        public TestRecord(byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TestRecord) {
                TestRecord record = (TestRecord) obj;

                return Arrays.equals(buffer, record.buffer);
            } else {
                return false;
            }
        }
    }

    public static class TestRecordSerializer extends Serializer<TestRecord>
            implements Serializable {

        private static final long serialVersionUID = 6971996565421454985L;

        @Override
        public void write(Kryo kryo, Output output, TestRecord object) {
            output.writeInt(object.buffer.length);
            output.write(object.buffer);
        }

        @Override
        public TestRecord read(Kryo kryo, Input input, Class<TestRecord> type) {
            int length = input.readInt();
            byte[] buffer = input.readBytes(length);

            return new TestRecord(buffer);
        }
    }

    public static class TestDataOutputView implements DataOutputView {

        private byte[] buffer;
        private int position;

        public TestDataOutputView(int size) {
            buffer = new byte[size];
            position = 0;
        }

        public void clear() {
            position = 0;
        }

        public byte[] getBuffer() {
            return buffer;
        }

        public void checkSize(int numBytes) throws EOFException {
            if (position + numBytes > buffer.length) {
                throw new EOFException();
            }
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            checkSize(numBytes);

            position += numBytes;
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {
            checkSize(numBytes);

            byte[] tempBuffer = new byte[numBytes];

            source.readFully(tempBuffer);

            System.arraycopy(tempBuffer, 0, buffer, position, numBytes);

            position += numBytes;
        }

        @Override
        public void write(int b) throws IOException {
            checkSize(4);

            position += 4;
        }

        @Override
        public void write(byte[] b) throws IOException {
            checkSize(b.length);

            System.arraycopy(b, 0, buffer, position, b.length);
            position += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkSize(len);

            System.arraycopy(b, off, buffer, position, len);

            position += len;
        }

        @Override
        public void writeBoolean(boolean v) throws IOException {
            checkSize(1);
            position += 1;
        }

        @Override
        public void writeByte(int v) throws IOException {
            checkSize(1);

            buffer[position] = (byte) v;

            position++;
        }

        @Override
        public void writeShort(int v) throws IOException {
            checkSize(2);

            position += 2;
        }

        @Override
        public void writeChar(int v) throws IOException {
            checkSize(1);
            position++;
        }

        @Override
        public void writeInt(int v) throws IOException {
            checkSize(4);

            position += 4;
        }

        @Override
        public void writeLong(long v) throws IOException {
            checkSize(8);
            position += 8;
        }

        @Override
        public void writeFloat(float v) throws IOException {
            checkSize(4);
            position += 4;
        }

        @Override
        public void writeDouble(double v) throws IOException {
            checkSize(8);
            position += 8;
        }

        @Override
        public void writeBytes(String s) throws IOException {
            byte[] sBuffer = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
            checkSize(sBuffer.length);
            System.arraycopy(sBuffer, 0, buffer, position, sBuffer.length);
            position += sBuffer.length;
        }

        @Override
        public void writeChars(String s) throws IOException {
            byte[] sBuffer = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
            checkSize(sBuffer.length);
            System.arraycopy(sBuffer, 0, buffer, position, sBuffer.length);
            position += sBuffer.length;
        }

        @Override
        public void writeUTF(String s) throws IOException {
            byte[] sBuffer = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
            checkSize(sBuffer.length);
            System.arraycopy(sBuffer, 0, buffer, position, sBuffer.length);
            position += sBuffer.length;
        }
    }
}
