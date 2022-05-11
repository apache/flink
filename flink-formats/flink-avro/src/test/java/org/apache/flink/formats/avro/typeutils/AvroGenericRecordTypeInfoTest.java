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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.formats.avro.utils.AvroTestUtils;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

/** Test for {@link GenericRecordAvroTypeInfo}. */
public class AvroGenericRecordTypeInfoTest
        extends TypeInformationTestBase<GenericRecordAvroTypeInfo> {

    @Override
    protected GenericRecordAvroTypeInfo[] getTestData() {
        return new GenericRecordAvroTypeInfo[] {
            new GenericRecordAvroTypeInfo(AvroTestUtils.getSmallSchema()),
            new GenericRecordAvroTypeInfo(AvroTestUtils.getLargeSchema())
        };
    }

    @Test
    void testAvroByDefault() {
        final TypeSerializer<GenericRecord> serializer =
                new GenericRecordAvroTypeInfo(AvroTestUtils.getLargeSchema())
                        .createSerializer(new ExecutionConfig());
        assertTrue(serializer instanceof AvroSerializer);
    }
}
