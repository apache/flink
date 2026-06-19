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

import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/** An {@link Encoder} that writes data to a {@link DataOutput}. */
public final class DataOutputEncoder extends Encoder {

    private DataOutput out;

    public void setOut(DataOutput out) {
        this.out = out;
    }

    @Override
    public void flush() throws IOException {}

    // --------------------------------------------------------------------------------------------
    // primitives
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeNull() {}

    @Override
    public void writeBoolean(boolean b) throws IOException {
        out.writeBoolean(b);
    }

    @Override
    public void writeInt(int n) throws IOException {
        out.writeInt(n);
    }

    @Override
    public void writeLong(long n) throws IOException {
        out.writeLong(n);
    }

    @Override
    public void writeFloat(float f) throws IOException {
        out.writeFloat(f);
    }

    @Override
    public void writeDouble(double d) throws IOException {
        out.writeDouble(d);
    }

    @Override
    public void writeEnum(int e) throws IOException {
        out.writeInt(e);
    }

    // --------------------------------------------------------------------------------------------
    // bytes
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeFixed(byte[] bytes, int start, int len) throws IOException {
        out.write(bytes, start, len);
    }

    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
        out.writeInt(len);
        if (len > 0) {
            out.write(bytes, start, len);
        }
    }

    @Override
    public void writeBytes(ByteBuffer bytes) throws IOException {
        int num = bytes.remaining();
        out.writeInt(num);

        if (num > 0) {
            writeFixed(bytes);
        }
    }

    // --------------------------------------------------------------------------------------------
    // strings
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeString(String str) throws IOException {
        byte[] bytes = Utf8.getBytesFor(str);
        writeBytes(bytes, 0, bytes.length);
    }

    @Override
    public void writeString(Utf8 utf8) throws IOException {
        writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
    }

    // --------------------------------------------------------------------------------------------
    // collection types
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeArrayStart() {}

    @Override
    public void setItemCount(long itemCount) throws IOException {
        if (itemCount > 0) {
            writeVarLongCount(out, itemCount);
        }
    }

    @Override
    public void startItem() {}

    @Override
    public void writeArrayEnd() throws IOException {
        // write a single byte 0, shortcut for a var-length long of 0
        out.write(0);
    }

    @Override
    public void writeMapStart() {}

    @Override
    public void writeMapEnd() throws IOException {
        // write a single byte 0, shortcut for a var-length long of 0
        out.write(0);
    }

    // --------------------------------------------------------------------------------------------
    // union
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeIndex(int unionIndex) throws IOException {
        out.writeInt(unionIndex);
    }

    // --------------------------------------------------------------------------------------------
    // utils
    // --------------------------------------------------------------------------------------------

    public static void writeVarLongCount(DataOutput out, long val) throws IOException {
        if (val < 0) {
            throw new IOException("Illegal count (must be non-negative): " + val);
        }

        while ((val & ~0x7FL) != 0) {
            out.write(((int) val) | 0x80);
            val >>>= 7;
        }
        out.write((int) val);
    }
}
