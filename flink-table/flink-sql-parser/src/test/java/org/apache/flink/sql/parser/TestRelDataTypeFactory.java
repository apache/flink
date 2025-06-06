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

package org.apache.flink.sql.parser;

import org.apache.flink.table.calcite.ExtendedRelTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** {@link RelDataTypeFactory} for testing purposes. */
final class TestRelDataTypeFactory extends SqlTypeFactoryImpl implements ExtendedRelTypeFactory {

    TestRelDataTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public RelDataType createRawType(String className, String serializerString) {
        return canonize(new DummyRawType(className, serializerString));
    }

    @Override
    public RelDataType createStructuredType(
            String className, List<RelDataType> typeList, List<String> fieldNameList) {
        return canonize(new DummyStructuredType(className, typeList, fieldNameList));
    }

    private static class DummyRawType extends RelDataTypeImpl {

        private final String className;
        private final String serializerString;

        DummyRawType(String className, String serializerString) {
            this.className = className;
            this.serializerString = serializerString;
            computeDigest();
        }

        @Override
        protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append("RAW('");
            sb.append(className);
            sb.append("', '");
            sb.append(serializerString);
            sb.append("')");
        }
    }

    private static class DummyStructuredType extends RelDataTypeImpl {

        private final String className;
        private final List<RelDataType> typeList;
        private final List<String> fieldNameList;

        DummyStructuredType(
                String className, List<RelDataType> typeList, List<String> fieldNameList) {
            this.className = className;
            this.typeList = typeList;
            this.fieldNameList = fieldNameList;
        }

        @Override
        protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append("STRUCTURED<'");
            sb.append(className);
            if (!typeList.isEmpty()) {
                sb.append("', ");
                sb.append(
                        IntStream.range(0, typeList.size())
                                .mapToObj(
                                        pos ->
                                                String.format(
                                                        "%s %s",
                                                        fieldNameList.get(pos), typeList.get(pos)))
                                .collect(Collectors.joining(", ")));
            }
            sb.append(">");
        }
    }
}
