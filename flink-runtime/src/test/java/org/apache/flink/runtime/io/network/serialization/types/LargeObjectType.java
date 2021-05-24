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

package org.apache.flink.runtime.io.network.serialization.types;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.testutils.serialization.types.SerializationTestType;

import java.io.IOException;
import java.util.Random;

/** A large {@link SerializationTestType}. */
public class LargeObjectType implements SerializationTestType {

    private static final int MIN_LEN = 12 * 1000 * 1000;

    private static final int MAX_LEN = 40 * 1000 * 1000;

    private int len;

    public LargeObjectType() {
        this.len = 0;
    }

    public LargeObjectType(int len) {
        this.len = len;
    }

    @Override
    public LargeObjectType getRandom(Random rnd) {
        int len = rnd.nextInt(MAX_LEN - MIN_LEN) + MIN_LEN;
        return new LargeObjectType(len);
    }

    @Override
    public int length() {
        return len + 4;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(len);
        for (int i = 0; i < len / 8; i++) {
            out.writeLong(i);
        }
        for (int i = 0; i < len % 8; i++) {
            out.write(i);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        final int len = in.readInt();
        this.len = len;

        for (int i = 0; i < len / 8; i++) {
            if (in.readLong() != i) {
                throw new IOException("corrupt serialization");
            }
        }

        for (int i = 0; i < len % 8; i++) {
            if (in.readByte() != i) {
                throw new IOException("corrupt serialization");
            }
        }
    }

    @Override
    public int hashCode() {
        return len;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof LargeObjectType) && ((LargeObjectType) obj).len == this.len;
    }

    @Override
    public String toString() {
        return "Large Object " + len;
    }
}
