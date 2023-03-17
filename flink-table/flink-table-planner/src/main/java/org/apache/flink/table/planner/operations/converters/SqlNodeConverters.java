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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Registry of SqlNode converters. */
public class SqlNodeConverters {

    private static final Map<Class<?>, SqlNodeConverter<?>> CONVERTERS = new HashMap<>();

    static {
        // register all the converters here
        register(new SqlCreateCatalogConverter());
    }

    /**
     * Convert the given validated SqlNode into Operation if there is a registered converter for the
     * node.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Optional<Operation> convertSqlNode(
            SqlNode validatedSqlNode, ConvertContext context) {
        SqlNodeConverter converter = CONVERTERS.get(validatedSqlNode.getClass());
        if (converter != null) {
            return Optional.of(converter.convertSqlNode(validatedSqlNode, context));
        } else {
            return Optional.empty();
        }
    }

    private static void register(SqlNodeConverter<?> converter) {
        // extract the parameter type of the converter class
        TypeInformation<?> typeInfo =
                TypeExtractor.createTypeInfo(
                        converter, SqlNodeConverter.class, converter.getClass(), 0);
        CONVERTERS.put(typeInfo.getTypeClass(), converter);
    }
}
