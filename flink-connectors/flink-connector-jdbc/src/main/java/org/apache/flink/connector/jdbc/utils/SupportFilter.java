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

package org.apache.flink.connector.jdbc.utils;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.jdbc.utils.FilterClause.AND;
import static org.apache.flink.connector.jdbc.utils.FilterClause.EQ;
import static org.apache.flink.connector.jdbc.utils.FilterClause.GT;
import static org.apache.flink.connector.jdbc.utils.FilterClause.GT_EQ;
import static org.apache.flink.connector.jdbc.utils.FilterClause.IS_NOT_NULL;
import static org.apache.flink.connector.jdbc.utils.FilterClause.IS_NULL;
import static org.apache.flink.connector.jdbc.utils.FilterClause.LIKE;
import static org.apache.flink.connector.jdbc.utils.FilterClause.LT;
import static org.apache.flink.connector.jdbc.utils.FilterClause.LT_EQ;
import static org.apache.flink.connector.jdbc.utils.FilterClause.NOT;
import static org.apache.flink.connector.jdbc.utils.FilterClause.NOT_EQ;
import static org.apache.flink.connector.jdbc.utils.FilterClause.OR;

/** SQL filters that support push down. */
public class SupportFilter {
    private static final Map<FunctionDefinition, FilterClause> FILTERS = new HashMap<>();

    static {
        FILTERS.put(BuiltInFunctionDefinitions.EQUALS, EQ);
        FILTERS.put(BuiltInFunctionDefinitions.NOT_EQUALS, NOT_EQ);
        FILTERS.put(BuiltInFunctionDefinitions.GREATER_THAN, GT);
        FILTERS.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, GT_EQ);
        FILTERS.put(BuiltInFunctionDefinitions.LESS_THAN, LT);
        FILTERS.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, LT_EQ);
        FILTERS.put(BuiltInFunctionDefinitions.IS_NULL, IS_NULL);
        FILTERS.put(BuiltInFunctionDefinitions.IS_NOT_NULL, IS_NOT_NULL);
        FILTERS.put(BuiltInFunctionDefinitions.AND, AND);
        FILTERS.put(BuiltInFunctionDefinitions.OR, OR);
        FILTERS.put(BuiltInFunctionDefinitions.LIKE, LIKE);
        FILTERS.put(BuiltInFunctionDefinitions.NOT, NOT);
    }

    private SupportFilter() {}

    public static boolean contain(FunctionDefinition function) {
        return FILTERS.containsKey(function);
    }

    public static FilterClause getFilterClause(FunctionDefinition function) {
        return FILTERS.get(function);
    }
}
