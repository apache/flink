/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.impl.ParseException;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlConformance;

import java.lang.reflect.InvocationTargetException;

/**
 * FlinkParserWrapper proxy TableApiIdentifier and FACTORY invoke to {@link FlinkSqlParserImpl}. as
 * well as {@link FlinkHiveSqlParserImpl}.
 */
@Internal
public class FlinkParserWrapper {
    private static final String FLINK_HIVE_SQL_PARSER_CLASS =
            "org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl";
    private final SqlAbstractParserImpl parserImpl;

    public FlinkParserWrapper(SqlAbstractParserImpl parserImpl) {
        this.parserImpl = parserImpl;
    }

    /**
     * Load SQL parser TableApiIdentifier method generated from Parser.jj by JavaCC.
     *
     * @return calcite SqlIdentifier
     */
    public SqlIdentifier tableApiIdentifier() throws ParseException {
        if (parserImpl instanceof FlinkSqlParserImpl) {
            return ((FlinkSqlParserImpl) parserImpl).TableApiIdentifier();
        } else {
            try {
                return (SqlIdentifier)
                        parserImpl.getClass().getMethod("TableApiIdentifier").invoke(parserImpl);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new IllegalArgumentException(
                        "Unrecognized sql parser type " + parserImpl.getClass().getName());
            }
        }
    }

    /**
     * Load SQL parser FACTORY field generated from Parser.jj by JavaCC.
     *
     * @param conformance FlinkSqlConformance type
     * @return calcite SqlParserImplFactory
     */
    public static SqlParserImplFactory factory(SqlConformance conformance) {
        if (conformance == FlinkSqlConformance.HIVE) {
            try {
                return (SqlParserImplFactory)
                        Class.forName(
                                        FLINK_HIVE_SQL_PARSER_CLASS,
                                        true,
                                        Thread.currentThread().getContextClassLoader())
                                .getDeclaredField("FACTORY")
                                .get(null);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to load " + FLINK_HIVE_SQL_PARSER_CLASS, e);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(
                        "Unable to find FACTORY field in " + FLINK_HIVE_SQL_PARSER_CLASS, e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Unable to access FACTORY field in " + FLINK_HIVE_SQL_PARSER_CLASS, e);
            }
        } else {
            return FlinkSqlParserImpl.FACTORY;
        }
    }
}
