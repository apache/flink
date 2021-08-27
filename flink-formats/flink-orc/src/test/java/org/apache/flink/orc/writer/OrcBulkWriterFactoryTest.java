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

package org.apache.flink.orc.writer;

import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.orc.data.Record;
import org.apache.flink.orc.vector.RecordVectorizer;
import org.apache.flink.orc.vector.Vectorizer;

import org.apache.hadoop.fs.Path;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** Tests the behavior of {@link OrcBulkWriterFactory}. */
public class OrcBulkWriterFactoryTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testNotOverrideInMemoryManager() throws IOException {
        TestMemoryManager memoryManager = new TestMemoryManager();
        OrcBulkWriterFactory<Record> factory =
                new TestOrcBulkWriterFactory<>(
                        new RecordVectorizer("struct<_col0:string,_col1:int>"), memoryManager);
        factory.create(new LocalDataOutputStream(temporaryFolder.newFile()));
        factory.create(new LocalDataOutputStream(temporaryFolder.newFile()));

        List<Path> addedWriterPath = memoryManager.getAddedWriterPath();
        assertEquals(2, addedWriterPath.size());
        assertNotEquals(addedWriterPath.get(0), addedWriterPath.get(1));
    }

    private static class TestOrcBulkWriterFactory<T> extends OrcBulkWriterFactory<T> {

        private final MemoryManager memoryManager;

        public TestOrcBulkWriterFactory(Vectorizer<T> vectorizer, MemoryManager memoryManager) {
            super(vectorizer);
            this.memoryManager = checkNotNull(memoryManager);
        }

        @Override
        protected OrcFile.WriterOptions getWriterOptions() {
            OrcFile.WriterOptions options = super.getWriterOptions();
            options.memory(memoryManager);
            return options;
        }
    }

    private static class TestMemoryManager implements MemoryManager {
        private final List<Path> addedWriterPath = new ArrayList<>();

        @Override
        public void addWriter(Path path, long requestedAllocation, Callback callback) {
            addedWriterPath.add(path);
        }

        public List<Path> getAddedWriterPath() {
            return addedWriterPath;
        }

        @Override
        public void removeWriter(Path path) {}

        @Override
        public void addedRow(int rows) {}
    }
}
