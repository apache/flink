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

import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class PrimitiveDataTypeTest {

    private PipedInputStream in;
    private PipedOutputStream out;

    private DataInputView mIn;
    private DataOutputView mOut;

    @Before
    public void setup() throws Exception {
        in = new PipedInputStream(1000);
        out = new PipedOutputStream(in);
        mIn = new DataInputViewStreamWrapper(in);
        mOut = new DataOutputViewStreamWrapper(out);
    }

    @Test
    void testIntValue() {
        IntValue int0 = new IntValue(10);
        // test value retrieval
        assertThat(int0.getValue()).isEqualTo(10);
        // test value comparison
        IntValue int1 = new IntValue(10);
        IntValue int2 = new IntValue(-10);
        IntValue int3 = new IntValue(20);
        assertThat(int0.compareTo(int0)).isEqualTo(0);
        assertThat(int0.compareTo(int1)).isEqualTo(0);
        assertThat(int0.compareTo(int2)).isEqualTo(1);
        assertThat(int0.compareTo(int3)).isEqualTo(-1);
        // test stream output and retrieval
        try {
            int0.write(mOut);
            int2.write(mOut);
            int3.write(mOut);
            IntValue int1n = new IntValue();
            IntValue int2n = new IntValue();
            IntValue int3n = new IntValue();
            int1n.read(mIn);
            int2n.read(mIn);
            int3n.read(mIn);
            assertThat(int0.compareTo(int1n)).isEqualTo(0);
            assertThat(int2.compareTo(int2n)).isEqualTo(0);
            assertThat(int3.compareTo(int3n)).isEqualTo(0);
            assertThat(int0.getValue()).isEqualTo(int1n.getValue());
            assertThat(int2.getValue()).isEqualTo(int2n.getValue());
            assertThat(int3.getValue()).isEqualTo(int3n.getValue());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testDoubleValue() {
        DoubleValue double0 = new DoubleValue(10.2);
        // test value retrieval
        assertThat(10.2, double0.getValue(), 0.0001);
        // test value comparison
        DoubleValue double1 = new DoubleValue(10.2);
        DoubleValue double2 = new DoubleValue(-10.5);
        DoubleValue double3 = new DoubleValue(20.2);
        assertThat(double0.compareTo(double0)).isEqualTo(0);
        assertThat(double0.compareTo(double1)).isEqualTo(0);
        assertThat(double0.compareTo(double2)).isEqualTo(1);
        assertThat(double0.compareTo(double3), -1);
        // test stream output and retrieval
        try {
            double0.write(mOut);
            double2.write(mOut);
            double3.write(mOut);
            DoubleValue double1n = new DoubleValue();
            DoubleValue double2n = new DoubleValue();
            DoubleValue double3n = new DoubleValue();
            double1n.read(mIn);
            double2n.read(mIn);
            double3n.read(mIn);
            assertThat(double0.compareTo(double1n), 0);
            assertThat(double0.getValue(), double1n.getValue(), 0.0001);
            assertThat(double2.compareTo(double2n), 0);
            assertThat(double2.getValue(), double2n.getValue(), 0.0001);
            assertThat(double3.compareTo(double3n), 0);
            assertThat(double3.getValue(), double3n.getValue(), 0.0001);
        } catch (Exception e) {
            assertThat(false).isTrue();
        }
    }

    @Test
    void testStringValue() {
        StringValue string0 = new StringValue("This is a test");
        StringValue stringThis = new StringValue("This");
        StringValue stringIsA = new StringValue("is a");
        // test value retrieval
        assertThat(string0.toString()).isEqualTo("This is a test");
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
        assertThat(string0.compareTo(string0) == 0).isTrue();
        assertThat(string0.compareTo(string1) == 0).isTrue();
        assertThat(string0.compareTo(string2) > 0).isTrue();
        assertThat(string0.compareTo(string3) < 0).isTrue();
        assertThat(stringThis.equals(chars5)).isTrue();
        assertThat(stringThis.compareTo(string5) == 0).isTrue();
        assertThat(string0.compareTo(string6) == 0).isTrue();
        assertThat(stringIsA.compareTo(string7) == 0).isTrue();
        string7.setValue("This is a test");
        assertThat(stringIsA.compareTo(string7) > 0).isTrue();
        assertThat(string0.compareTo(string7) == 0).isTrue();
        string7.setValue("is a");
        assertThat(stringIsA.compareTo(string7) == 0).isTrue();
        assertThat(string0.compareTo(string7) < 0).isTrue();
        assertThat(string7.hashCode()).isEqualTo(stringIsA.hashCode());
        assertThat(string7.length()).isEqualTo(4);
        assertThat(string7.getValue()).isEqualTo("is a");
        assertThat(string8.length()).isEqualTo(0);
        assertThat(string8.getValue()).isEqualTo("");
        assertThat(string7.charAt(1)).isEqualTo('s');
        try {
            string7.charAt(5);
            fail("Exception should have been thrown when accessing characters out of bounds.");
        } catch (IndexOutOfBoundsException iOOBE) {
            // expected
        }

        // test stream out/input
        try {
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
            assertThat(0).isEqualTo(string0.compareTo(string1n));
            assertThat(0).isEqualTo(string4.compareTo(string4n));
            assertThat(0).isEqualTo(string2.compareTo(string2n));
            assertThat(0).isEqualTo(string3.compareTo(string3n));
            assertThat(0).isEqualTo(string7.compareTo(string7n));
            assertThat(string1n.toString()).isEqualTo(string0.toString());
            assertThat(string4n.toString()).isEqualTo(string4.toString());
            assertThat(string2n.toString()).isEqualTo(string2.toString());
            assertThat(string3n.toString()).isEqualTo(string3.toString());
            assertThat(string7n.toString()).isEqualTo(string7.toString());
            try {
                string7n.charAt(5);
                fail("Exception should have been thrown when accessing characters out of bounds.");
            } catch (IndexOutOfBoundsException iOOBE) {
                // expected
            }

        } catch (Exception e) {
            assertThat(false).isTrue();
        }
    }

    @Test
    void testPactNull() {

        final NullValue pn1 = new NullValue();
        final NullValue pn2 = new NullValue();

        assertThat(pn2).isEqualTo(pn1);
        assertThat(pn1).isEqualTo(pn2);

        assertThat(pn1.equals(null)).isFalse();

        // test serialization
        final NullValue pn = new NullValue();
        final int numWrites = 13;

        try {
            // write it multiple times
            for (int i = 0; i < numWrites; i++) {
                pn.write(mOut);
            }

            // read it multiple times
            for (int i = 0; i < numWrites; i++) {
                pn.read(mIn);
            }

            assertThat(in.available()).isEqualTo(0);
        } catch (IOException ioex) {
            fail("An exception occurred in the testcase: " + ioex.getMessage());
        }
    }
}
