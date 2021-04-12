/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.testutils.CustomEqualityMatcher;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Abstract test base for {@link ArrowReader} and {@link ArrowWriter}.
 *
 * @param <T> the elment type.
 */
public abstract class ArrowReaderWriterTestBase<T> {

    private final DeeplyEqualsChecker checker;

    ArrowReaderWriterTestBase() {
        this.checker = new DeeplyEqualsChecker();
    }

    ArrowReaderWriterTestBase(DeeplyEqualsChecker checker) {
        this.checker = Preconditions.checkNotNull(checker);
    }

    @Test
    public void testBasicFunctionality() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Tuple2<ArrowWriter<T>, ArrowStreamWriter> tuple2 = createArrowWriter(baos);
            ArrowWriter<T> arrowWriter = tuple2.f0;
            ArrowStreamWriter arrowStreamWriter = tuple2.f1;

            T[] testData = getTestData();
            for (T value : testData) {
                arrowWriter.write(value);
            }
            arrowWriter.finish();
            arrowStreamWriter.writeBatch();

            ArrowReader<T> arrowReader =
                    createArrowReader(new ByteArrayInputStream(baos.toByteArray()));
            for (int i = 0; i < testData.length; i++) {
                T deserialized = arrowReader.read(i);
                assertThat(
                        "Deserialized value is wrong.",
                        deserialized,
                        CustomEqualityMatcher.deeplyEquals(testData[i]).withChecker(checker));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    public abstract ArrowReader<T> createArrowReader(InputStream inputStream) throws IOException;

    public abstract Tuple2<ArrowWriter<T>, ArrowStreamWriter> createArrowWriter(
            OutputStream outputStream) throws IOException;

    public abstract T[] getTestData();
}
