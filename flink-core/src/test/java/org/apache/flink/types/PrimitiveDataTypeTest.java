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

package org.apache.flink.types;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatComparable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class PrimitiveDataTypeTest {

    private PipedInputStream in;
    private PipedOutputStream out;

    private DataInputView mIn;
    private DataOutputView mOut;

    @BeforeEach
    void setup() throws Exception {
        in = new PipedInputStream(1000);
        out = new PipedOutputStream(in);
        mIn = new DataInputViewStreamWrapper(in);
        mOut = new DataOutputViewStreamWrapper(out);
    }

    @Test
    void testIntValue() throws IOException {
        IntValue int0 = new IntValue(10);
        // test value retrieval
        assertThat(int0.getValue()).isEqualTo(10);
        // test value comparison
        IntValue int1 = new IntValue(10);
        IntValue int2 = new IntValue(-10);
        IntValue int3 = new IntValue(20);
        assertThat(int0).isEqualByComparingTo(int0);
        assertThat(int0).isEqualByComparingTo(int1);
        assertThat(int0.compareTo(int2)).isOne();
        assertThat(int0.compareTo(int3)).isEqualTo(-1);
        // test stream output and retrieval
        int0.write(mOut);
        int2.write(mOut);
        int3.write(mOut);
        IntValue int1n = new IntValue();
        IntValue int2n = new IntValue();
        IntValue int3n = new IntValue();
        int1n.read(mIn);
        int2n.read(mIn);
        int3n.read(mIn);
        assertThat(int0).isEqualByComparingTo(int1n);
        assertThat(int1n.getValue()).isEqualTo(int0.getValue());
        assertThat(int2).isEqualByComparingTo(int2n);
        assertThat(int2n.getValue()).isEqualTo(int2.getValue());
        assertThat(int3).isEqualByComparingTo(int3n);
        assertThat(int3n.getValue()).isEqualTo(int3.getValue());
    }

    @Test
    void testDoubleValue() throws IOException {
        DoubleValue double0 = new DoubleValue(10.2);
        // test value retrieval
        assertThat(double0.getValue()).isCloseTo(10.2, within(0.0001));
        // test value comparison
        DoubleValue double1 = new DoubleValue(10.2);
        DoubleValue double2 = new DoubleValue(-10.5);
        DoubleValue double3 = new DoubleValue(20.2);
        assertThat(double0).isEqualByComparingTo(double0);
        assertThat(double0).isEqualByComparingTo(double1);
        assertThat(double0.compareTo(double2)).isOne();
        assertThat(double0.compareTo(double3)).isEqualTo(-1);
        // test stream output and retrieval
        double0.write(mOut);
        double2.write(mOut);
        double3.write(mOut);
        DoubleValue double1n = new DoubleValue();
        DoubleValue double2n = new DoubleValue();
        DoubleValue double3n = new DoubleValue();
        double1n.read(mIn);
        double2n.read(mIn);
        double3n.read(mIn);
        assertThat(double0).isEqualByComparingTo(double1n);
        assertThat(double1n.getValue()).isCloseTo(double0.getValue(), within(0.0001));
        assertThat(double2).isEqualByComparingTo(double2n);
        assertThat(double2n.getValue()).isCloseTo(double2.getValue(), within(0.0001));
        assertThat(double3).isEqualByComparingTo(double3n);
        assertThat(double3n.getValue()).isCloseTo(double3.getValue(), within(0.0001));
    }

    @Test
    void testStringValue() throws IOException {
        StringValue string0 = new StringValue("This is a test");
        StringValue stringThis = new StringValue("This");
        StringValue stringIsA = new StringValue("is a");
        // test value retrieval
        assertThat((Object) string0).hasToString("This is a test");
        // test value comparison
        StringValue string1 = new StringValue("This is a test");
        StringValue string2 = new StringValue("This is a tesa");
        StringValue string3 = new StringValue("This is a tesz");
        StringValue string4 = new StringValue("Ünlaut ßtring µ avec é y ¢");
        CharSequence chars5 = string1.subSequence(0, 4);
        StringValue string5 = (StringValue) chars5;
        StringValue string6 = (StringValue) string0.subSequence(0, string0.length());
        StringValue string7 = (StringValue) string0.subSequence(5, 9);
        StringValue string8 = (StringValue) string0.subSequence(0, 0);
        assertThatComparable(string0).isEqualByComparingTo(string0);
        assertThatComparable(string1).isEqualByComparingTo(string0);
        assertThatComparable(string0).isGreaterThan(string2);
        assertThatComparable(string0).isLessThan(string3);
        assertThat(chars5).isEqualTo(stringThis);
        assertThatComparable(string5).isEqualByComparingTo(stringThis);
        assertThatComparable(string6).isEqualByComparingTo(string0);
        assertThatComparable(string7).isEqualByComparingTo(stringIsA);
        string7.setValue("This is a test");
        assertThatComparable(stringIsA).isGreaterThan(string7);
        assertThatComparable(string7).isEqualByComparingTo(string0);
        string7.setValue("is a");
        assertThatComparable(string7).isEqualByComparingTo(stringIsA);
        assertThatComparable(string0).isLessThan(string7);
        assertThat((Object) string7).hasSameHashCodeAs(stringIsA);
        assertThat(string7.length()).isEqualTo(4);
        assertThat(string7.getValue()).isEqualTo("is a");
        assertThat(string8.length()).isZero();
        assertThat((Object) string8).hasToString("");
        assertThat(string7.charAt(1)).isEqualTo('s');
        assertThatThrownBy(() -> string7.charAt(5)).isInstanceOf(IndexOutOfBoundsException.class);

        // test stream out/input
        string0.write(mOut);
        string4.write(mOut);
        string2.write(mOut);
        string3.write(mOut);
        string7.write(mOut);
        StringValue string1n = new StringValue();
        StringValue string2n = new StringValue();
        StringValue string3n = new StringValue();
        StringValue string4n = new StringValue();
        StringValue string7n = new StringValue();
        string1n.read(mIn);
        string4n.read(mIn);
        string2n.read(mIn);
        string3n.read(mIn);
        string7n.read(mIn);
        assertThatComparable(string1n).isEqualByComparingTo(string0);
        assertThat((Object) string1n).hasToString(string0.toString());
        assertThatComparable(string4n).isEqualByComparingTo(string4);
        assertThat((Object) string4n).hasToString(string4.toString());
        assertThatComparable(string2n).isEqualByComparingTo(string2);
        assertThat((Object) string2n).hasToString(string2.toString());
        assertThatComparable(string3n).isEqualByComparingTo(string3);
        assertThat((Object) string3n).hasToString(string3.toString());
        assertThatComparable(string7n).isEqualByComparingTo(string7);
        assertThat((Object) string7n).hasToString(string7.toString());
        assertThatThrownBy(() -> string7n.charAt(5)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testPactNull() throws IOException {

        final NullValue pn1 = new NullValue();
        final NullValue pn2 = new NullValue();

        assertThat(pn1).isEqualTo(pn2);
        assertThat(pn2).isEqualTo(pn1);

        assertThat(pn1).isNotNull();

        // test serialization
        final NullValue pn = new NullValue();
        final int numWrites = 13;

        // write it multiple times
        for (int i = 0; i < numWrites; i++) {
            pn.write(mOut);
        }

        // read it multiple times
        for (int i = 0; i < numWrites; i++) {
            pn.read(mIn);
        }

        assertThat(in.available()).isZero();
    }
}
