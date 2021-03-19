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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test cases for reading from Parquet files. */
@RunWith(Parameterized.class)
public class ParquetInputFormatTest extends TestUtil {
    private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

    @ClassRule public static TemporaryFolder tempRoot = new TemporaryFolder();

    public ParquetInputFormatTest(boolean useLegacyMode) {
        super(useLegacyMode);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReadWithoutParquetSchemaSpecified() throws IOException {
        Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> tuple =
                TestUtil.getSimpleRecordTestData();
        Path path =
                createTempParquetFile(
                        tempRoot.getRoot(),
                        SIMPLE_SCHEMA,
                        Collections.singletonList(tuple.f1),
                        getConfiguration());

        ParquetInputFormat<Row> inputFormat =
                new ParquetInputFormat(path, null) {
                    @Override
                    protected Row convert(Row row) {
                        return row;
                    }
                };
        inputFormat.setRuntimeContext(getMockRuntimeContext());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertEquals(1, splits.length);
        inputFormat.open(splits[0]);

        final Row record = inputFormat.nextRecord(null);
        assertNotNull(record);
        assertEquals(1L, record.getField(0));
        assertEquals("test_simple", record.getField(1));
        Long[] longArray = {1L};
        assertEquals(longArray, (Long[]) record.getField(2));
    }
}
