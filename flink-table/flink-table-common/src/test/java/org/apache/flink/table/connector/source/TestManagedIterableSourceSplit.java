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

package org.apache.flink.table.connector.source;

import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/** Managed {@link org.apache.flink.api.connector.source.SourceSplit} for testing. */
public class TestManagedIterableSourceSplit
        implements IteratorSourceSplit<RowData, Iterator<RowData>>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final Path filePath;

    private Iterator<RowData> iterator;

    public TestManagedIterableSourceSplit(String id, Path filePath) {
        this.id = id;
        this.filePath = filePath;
    }

    @Override
    public String splitId() {
        return id;
    }

    @Override
    public Iterator<RowData> getIterator() {
        if (iterator == null) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(filePath.getPath()));
                iterator =
                        Iterators.transform(
                                reader.lines().iterator(),
                                line ->
                                        GenericRowData.of(
                                                StringData.fromString(id),
                                                StringData.fromString(filePath.getPath()),
                                                StringData.fromString(line)));
            } catch (IOException e) {
                throw new FlinkRuntimeException(e);
            }
        }
        return iterator;
    }

    @Override
    public IteratorSourceSplit<RowData, Iterator<RowData>> getUpdatedSplitForIterator(
            Iterator<RowData> iterator) {
        TestManagedIterableSourceSplit recovered =
                new TestManagedIterableSourceSplit(this.id, this.filePath);
        recovered.iterator = this.iterator;
        return recovered;
    }

    @Override
    public String toString() {
        return "TestManagedIterableSourceSplit{"
                + "splitId='"
                + id
                + '\''
                + ", filePath="
                + filePath
                + '}';
    }

    public Path getFilePath() {
        return filePath;
    }
}
