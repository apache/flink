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
import org.apache.parquet.schema.MessageType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test cases for reading Map from Parquet files. */
@RunWith(Parameterized.class)
public class ParquetMapInputFormatTest extends TestUtil {
    private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

    @ClassRule public static TemporaryFolder tempRoot = new TemporaryFolder();

    public ParquetMapInputFormatTest(boolean useLegacyMode) {
        super(useLegacyMode);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReadMapFromNestedRecord() throws IOException {
        Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested =
                TestUtil.getNestedRecordTestData();
        Path path =
                createTempParquetFile(
                        tempRoot.getRoot(),
                        NESTED_SCHEMA,
                        Collections.singletonList(nested.f1),
                        getConfiguration());
        MessageType nestedType = getSchemaConverter().convert(NESTED_SCHEMA);

        ParquetMapInputFormat inputFormat = new ParquetMapInputFormat(path, nestedType);
        inputFormat.setRuntimeContext(getMockRuntimeContext());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertEquals(1, splits.length);
        inputFormat.open(splits[0]);

        Map map = inputFormat.nextRecord(null);
        assertNotNull(map);
        assertEquals(5, map.size());
        assertArrayEquals((Long[]) nested.f2.getField(3), (Long[]) map.get("arr"));
        assertArrayEquals((String[]) nested.f2.getField(4), (String[]) map.get("strArray"));

        Map<String, String> mapItem =
                (Map<String, String>) ((Map) map.get("nestedMap")).get("mapItem");
        assertEquals(2, mapItem.size());
        assertEquals("map", mapItem.get("type"));
        assertEquals("hashMap", mapItem.get("value"));

        List<Map<String, String>> nestedArray = (List<Map<String, String>>) map.get("nestedArray");
        assertEquals(1, nestedArray.size());
        assertEquals("color", nestedArray.get(0).get("type"));
        assertEquals(1L, nestedArray.get(0).get("value"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProjectedReadMapFromNestedRecord() throws IOException {
        Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested =
                TestUtil.getNestedRecordTestData();
        Path path =
                createTempParquetFile(
                        tempRoot.getRoot(),
                        NESTED_SCHEMA,
                        Collections.singletonList(nested.f1),
                        getConfiguration());
        MessageType nestedType = getSchemaConverter().convert(NESTED_SCHEMA);
        ParquetMapInputFormat inputFormat = new ParquetMapInputFormat(path, nestedType);

        inputFormat.selectFields(Collections.singletonList("nestedMap").toArray(new String[0]));
        inputFormat.setRuntimeContext(getMockRuntimeContext());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertEquals(1, splits.length);
        inputFormat.open(splits[0]);

        Map map = inputFormat.nextRecord(null);
        assertNotNull(map);
        assertEquals(1, map.size());

        Map<String, String> mapItem =
                (Map<String, String>) ((Map) map.get("nestedMap")).get("mapItem");
        assertEquals(2, mapItem.size());
        assertEquals("map", mapItem.get("type"));
        assertEquals("hashMap", mapItem.get("value"));
    }
}
