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

package org.apache.flink.testutils.serialization.types;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Random;

public class CharType implements SerializationTestType {

    private char value;

    public CharType() {
        this.value = 0;
    }

    private CharType(char value) {
        this.value = value;
    }

    @Override
    public CharType getRandom(Random rnd) {
        return new CharType((char) rnd.nextInt(10000));
    }

    @Override
    public int length() {
        return 2;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeChar(this.value);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.value = in.readChar();
    }

    @Override
    public int hashCode() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CharType) {
            CharType other = (CharType) obj;
            return this.value == other.value;
        } else {
            return false;
        }
    }
}
