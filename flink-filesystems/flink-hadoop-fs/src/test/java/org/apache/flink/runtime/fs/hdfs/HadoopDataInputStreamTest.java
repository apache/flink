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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests for the {@link HadoopDataInputStream}. */
class HadoopDataInputStreamTest {

    private FSDataInputStream verifyInputStream;
    private HadoopDataInputStream testInputStream;

    @Test
    void testSeekSkip() throws IOException {
        verifyInputStream =
                spy(
                        new FSDataInputStream(
                                new SeekableByteArrayInputStream(
                                        new byte[2 * HadoopDataInputStream.MIN_SKIP_BYTES])));
        testInputStream = new HadoopDataInputStream(verifyInputStream);
        seekAndAssert(10);
        seekAndAssert(10 + HadoopDataInputStream.MIN_SKIP_BYTES + 1);
        seekAndAssert(testInputStream.getPos() - 1);
        seekAndAssert(testInputStream.getPos() + 1);
        seekAndAssert(testInputStream.getPos() - HadoopDataInputStream.MIN_SKIP_BYTES);
        seekAndAssert(testInputStream.getPos());
        seekAndAssert(0);
        seekAndAssert(testInputStream.getPos() + HadoopDataInputStream.MIN_SKIP_BYTES);
        seekAndAssert(testInputStream.getPos() + HadoopDataInputStream.MIN_SKIP_BYTES - 1);

        assertThatThrownBy(() -> seekAndAssert(-1)).isInstanceOf(Exception.class);

        assertThatThrownBy(() -> seekAndAssert(-HadoopDataInputStream.MIN_SKIP_BYTES - 1))
                .isInstanceOf(Exception.class);
    }

    private void seekAndAssert(long seekPos) throws IOException {
        assertThat(testInputStream.getPos()).isEqualTo(verifyInputStream.getPos());
        long delta = seekPos - testInputStream.getPos();
        testInputStream.seek(seekPos);

        if (delta > 0L && delta <= HadoopDataInputStream.MIN_SKIP_BYTES) {
            verify(verifyInputStream, atLeastOnce()).skip(anyLong());
            verify(verifyInputStream, never()).seek(anyLong());
        } else if (delta != 0L) {
            verify(verifyInputStream, atLeastOnce()).seek(seekPos);
            verify(verifyInputStream, never()).skip(anyLong());
        } else {
            verify(verifyInputStream, never()).seek(anyLong());
            verify(verifyInputStream, never()).skip(anyLong());
        }

        assertThat(verifyInputStream.getPos()).isEqualTo(seekPos);

        reset(verifyInputStream);
    }

    private static final class SeekableByteArrayInputStream extends ByteArrayInputStreamWithPos
            implements Seekable, PositionedReadable {

        public SeekableByteArrayInputStream(byte[] buffer) {
            super(buffer);
        }

        @Override
        public void seek(long pos) throws IOException {
            setPosition((int) pos);
        }

        @Override
        public long getPos() throws IOException {
            return getPosition();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
