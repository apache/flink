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

package org.apache.flink.table.planner.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#HASHCODE}. */
public class GetHashCodeGenerator {

    public int getHashCode(List<LogicalType> logicalTypeList, RowData o1, ClassLoader classLoader) {
        LogicalType[] logicalTypes = new LogicalType[logicalTypeList.size()];
        int[] num = new int[logicalTypeList.size()];
        for (int i = 0; i < num.length; ++i) {
            num[i] = i;
        }
        for (int i = 0; i < logicalTypeList.size(); ++i) {
            logicalTypes[i] = logicalTypeList.get(i);
        }
        HashFunction hashFunc1 =
                HashCodeGenerator.generateRowHash(
                                new CodeGeneratorContext(new Configuration(), classLoader),
                                RowType.of(logicalTypes),
                                "name",
                                num)
                        .newInstance(classLoader);
        return hashFunc1.hashCode(o1);
    }
}
