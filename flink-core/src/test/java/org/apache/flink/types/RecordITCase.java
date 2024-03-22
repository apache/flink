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

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Random;

class RecordITCase {

    private static final long SEED = 354144423270432543L;
    private final Random rand = new Random(RecordITCase.SEED);

    private DataInputView in;
    private DataOutputView out;

    @BeforeEach
    void setUp() throws Exception {
        PipedInputStream pipedInput = new PipedInputStream(32 * 1024 * 1024);
        this.in = new DataInputViewStreamWrapper(pipedInput);
        this.out = new DataOutputViewStreamWrapper(new PipedOutputStream(pipedInput));
    }

    @Test
    void massiveRandomBlackBoxTests() throws Exception {
        // random test with records with a small number of fields
        for (int i = 0; i < 100000; i++) {
            final Value[] fields = RecordTest.createRandomValues(this.rand, 0, 32);
            RecordTest.blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
        }

        // random tests with records with a moderately large number of fields
        for (int i = 0; i < 2000; i++) {
            final Value[] fields = RecordTest.createRandomValues(this.rand, 20, 200);
            RecordTest.blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
        }

        // random tests with records with very many fields
        for (int i = 0; i < 200; i++) {
            final Value[] fields = RecordTest.createRandomValues(this.rand, 500, 2000);
            RecordTest.blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
        }
    }
}
