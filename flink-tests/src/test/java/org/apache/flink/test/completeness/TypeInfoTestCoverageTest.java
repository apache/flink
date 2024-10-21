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

package org.apache.flink.test.completeness;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.runtime.state.v2.ttl.TtlStateFactory;
import org.apache.flink.table.dataview.ListViewTypeInfo;
import org.apache.flink.table.dataview.MapViewTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyInstantTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyLocalDateTimeTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyTimestampTypeInfo;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.TimestampDataTypeInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/** Scans the class path for type information and checks if there is a test for it. */
public class TypeInfoTestCoverageTest extends TestLogger {

    @Test
    public void testTypeInfoTestCoverage() {
        Reflections reflections = new Reflections("org.apache.flink");

        Set<Class<? extends TypeInformation>> typeInfos =
                reflections.getSubTypesOf(TypeInformation.class);

        Set<String> typeInfoTestNames =
                reflections.getSubTypesOf(TypeInformationTestBase.class).stream()
                        .map(Class::getName)
                        .collect(Collectors.toSet());

        //  type info whitelist for TypeInformationTestBase test coverage (see FLINK-27725)
        final List<String> typeInfoTestBaseWhitelist =
                Arrays.asList(
                        LegacyTimestampTypeInfo.class.getName(),
                        InternalTypeInfo.class.getName(),
                        LegacyLocalDateTimeTypeInfo.class.getName(),
                        TimeIntervalTypeInfo.class.getName(),
                        TimeIndicatorTypeInfo.class.getName(),
                        TimestampDataTypeInfo.class.getName(),
                        MapViewTypeInfo.class.getName(),
                        LegacyInstantTypeInfo.class.getName(),
                        ListViewTypeInfo.class.getName(),
                        StringDataTypeInfo.class.getName(),
                        SortedMapTypeInfo.class.getName(),
                        ExternalTypeInfo.class.getName(),
                        BigDecimalTypeInfo.class.getName(),
                        DecimalDataTypeInfo.class.getName(),
                        GenericRecordAvroTypeInfo.class.getName(),
                        AvroTypeInfo.class.getName(),
                        TtlStateFactory.TtlTypeInformation.class.getName());

        // check if a test exists for each type information
        for (Class<? extends TypeInformation> typeInfo : typeInfos) {
            // we skip abstract classes and inner classes to skip type information defined in test
            // classes
            if (Modifier.isAbstract(typeInfo.getModifiers())
                    || Modifier.isPrivate(typeInfo.getModifiers())
                    || typeInfo.getName().contains("Test$")
                    || typeInfo.getName().contains("TestBase$")
                    || typeInfo.getName().contains("ITCase$")
                    || typeInfo.getName().contains("$$anon")
                    || typeInfo.getName().contains("queryablestate")) {
                continue;
            }

            final String testToFind = typeInfo.getName() + "Test";
            if (!typeInfoTestNames.contains(testToFind)
                    && !typeInfoTestBaseWhitelist.contains(typeInfo.getName())) {
                fail(
                        "Could not find test '"
                                + testToFind
                                + "' that covers '"
                                + typeInfo.getName()
                                + "'.");
            }
        }
    }
}
