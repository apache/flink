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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.ExpressionResolverBuilder;
import org.apache.flink.table.expressions.resolver.SqlExpressionResolver;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import java.util.Collections;
import java.util.Optional;

/**
 * Mock implementations of {@link ExpressionResolver}/{@link ExpressionResolverBuilder} for testing
 * purposes.
 */
public final class ExpressionResolverMocks {

    public static ExpressionResolverBuilder forSqlExpression(SqlExpressionResolver resolver) {
        return ExpressionResolver.resolverFor(
                new TableConfig(),
                name -> Optional.empty(),
                new FunctionLookupMock(Collections.emptyMap()),
                new DataTypeFactoryMock(),
                resolver);
    }

    public static ExpressionResolverBuilder dummyResolver() {
        return forSqlExpression(
                (sqlExpression, inputRowType, outputType) -> {
                    throw new UnsupportedOperationException();
                });
    }

    public static ExpressionResolverBuilder basicResolver(
            CatalogManager catalogManager, FunctionCatalog functionCatalog, Parser parser) {
        return ExpressionResolver.resolverFor(
                new TableConfig(),
                name -> Optional.empty(),
                functionCatalog.asLookup(parser::parseIdentifier),
                catalogManager.getDataTypeFactory(),
                parser::parseSqlExpression);
    }
}
