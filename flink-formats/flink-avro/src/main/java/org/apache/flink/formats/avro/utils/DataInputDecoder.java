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

import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/** A {@link Decoder} that reads from a {@link DataInput}. */
public class DataInputDecoder extends Decoder {

    private final Utf8 stringDecoder = new Utf8();

    private DataInput in;

    public void setIn(DataInput in) {
        this.in = in;
    }

    // --------------------------------------------------------------------------------------------
    // primitives
    // --------------------------------------------------------------------------------------------

    @Override
    public void readNull() {}

    @Override
    public boolean readBoolean() throws IOException {
        return in.readBoolean();
    }

    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return in.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return in.readDouble();
    }

    @Override
    public int readEnum() throws IOException {
        return readInt();
    }

    // --------------------------------------------------------------------------------------------
    // bytes
    // --------------------------------------------------------------------------------------------

    @Override
    public void readFixed(byte[] bytes, int start, int length) throws IOException {
        in.readFully(bytes, start, length);
    }

    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        int length = readInt();
        ByteBuffer result;
        if (old != null && length <= old.capacity() && old.hasArray()) {
            result = old;
            result.clear();
        } else {
            result = ByteBuffer.allocate(length);
        }
        in.readFully(result.array(), result.arrayOffset() + result.position(), length);
        result.limit(length);
        return result;
    }

    @Override
    public void skipFixed(int length) throws IOException {
        skipBytes(length);
    }

    @Override
    public void skipBytes() throws IOException {
        int num = readInt();
        skipBytes(num);
    }

    // --------------------------------------------------------------------------------------------
    // strings
    // --------------------------------------------------------------------------------------------

    @Override
    public Utf8 readString(Utf8 old) throws IOException {
        int length = readInt();
        Utf8 result = (old != null ? old : new Utf8());
        result.setByteLength(length);

        if (length > 0) {
            in.readFully(result.getBytes(), 0, length);
        }

        return result;
    }

    @Override
    public String readString() throws IOException {
        return readString(stringDecoder).toString();
    }

    @Override
    public void skipString() throws IOException {
        int len = readInt();
        skipBytes(len);
    }

    // --------------------------------------------------------------------------------------------
    // collection types
    // --------------------------------------------------------------------------------------------

    @Override
    public long readArrayStart() throws IOException {
        return readVarLongCount(in);
    }

    @Override
    public long arrayNext() throws IOException {
        return readVarLongCount(in);
    }

    @Override
    public long skipArray() throws IOException {
        return readVarLongCount(in);
    }

    @Override
    public long readMapStart() throws IOException {
        return readVarLongCount(in);
    }

    @Override
    public long mapNext() throws IOException {
        return readVarLongCount(in);
    }

    @Override
    public long skipMap() throws IOException {
        return readVarLongCount(in);
    }

    // --------------------------------------------------------------------------------------------
    // union
    // --------------------------------------------------------------------------------------------

    @Override
    public int readIndex() throws IOException {
        return readInt();
    }

    // --------------------------------------------------------------------------------------------
    // utils
    // --------------------------------------------------------------------------------------------

    private void skipBytes(int num) throws IOException {
        while (num > 0) {
            num -= in.skipBytes(num);
        }
    }

    public static long readVarLongCount(DataInput in) throws IOException {
        long value = in.readUnsignedByte();

        if ((value & 0x80) == 0) {
            return value;
        } else {
            long curr;
            int shift = 7;
            value = value & 0x7f;
            while (((curr = in.readUnsignedByte()) & 0x80) != 0) {
                value |= (curr & 0x7f) << shift;
                shift += 7;
            }
            value |= curr << shift;
            return value;
        }
    }
}
