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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.local.AbstractRecoverableFsDataOutputStreamTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Unit tests for {@link HadoopRecoverableFsDataOutputStream}. */
public class HadoopRecoverableFsDataOutputStreamTest
        extends AbstractRecoverableFsDataOutputStreamTest {

    @Override
    public Tuple2<RecoverableFsDataOutputStream, Closeable> createTestInstance(
            Path target, Path temp, List<Event> testLog) throws IOException {
        final TestFSDataOutputStream fos =
                new TestFSDataOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(temp)), testLog);

        final HadoopRecoverableFsDataOutputStream testOutStreamInstance =
                new HadoopRecoverableFsDataOutputStream(
                        FileSystem.getLocal(new Configuration()),
                        new org.apache.hadoop.fs.Path(target.toUri()),
                        new org.apache.hadoop.fs.Path(temp.toUri()),
                        fos);

        return new Tuple2<>(testOutStreamInstance, fos::actualClose);
    }

    private static class TestFSDataOutputStream extends FSDataOutputStream {

        private final List<Event> events;

        public TestFSDataOutputStream(OutputStream out, List<Event> events) throws IOException {
            super(out, new FileSystem.Statistics("test_stats"));
            this.events = events;
        }

        @Override
        public void hflush() throws IOException {
            super.hflush();
            events.add(Event.FLUSH);
        }

        @Override
        public void hsync() throws IOException {
            super.hsync();
            events.add(Event.SYNC);
        }

        @Override
        public void close() {
            events.add(Event.CLOSE);
            // Do nothing on close.
        }

        public void actualClose() throws IOException {
            super.close();
        }
    }
}
