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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.core.memory.DataInputView;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link DataInputViewStream}. */
class DataInputViewStreamTest {

    @Test
    void testSkip() throws IOException {
        final TestInputStream inputStream = new TestInputStream();
        try (TestDataInputView dataInputView = new TestDataInputView(inputStream)) {
            try (DataInputViewStream dataInputViewStream = new DataInputViewStream(dataInputView)) {
                assertThat(dataInputViewStream.skip(1)).isOne();
                assertThat(inputStream.skipped).isOne();

                final long bigNumberToSkip = 1024L + 2L * Integer.MAX_VALUE;
                assertThat(dataInputViewStream.skip(bigNumberToSkip)).isEqualTo(bigNumberToSkip);
                assertThat(inputStream.skipped).isEqualTo(1 + bigNumberToSkip);
            }
        }
    }

    /** Test implementation of {@link DataInputView}. */
    private static class TestDataInputView extends DataInputStream implements DataInputView {

        TestDataInputView(InputStream in) {
            super(in);
        }

        @Override
        public void skipBytesToRead(int numBytes) throws IOException {
            throw new UnsupportedOperationException("Not properly implemented.");
        }
    }

    /** Test implementation of {@link InputStream}. */
    private static class TestInputStream extends InputStream {

        long skipped = 0;

        @Override
        public int read() throws IOException {
            return 0;
        }

        @Override
        public long skip(long n) {
            skipped += n;
            return n;
        }
    }
}
