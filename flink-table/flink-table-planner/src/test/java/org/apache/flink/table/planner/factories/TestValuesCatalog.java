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

package org.apache.flink.table.planner.factories;

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FunctionDefinitionFactory;

import java.util.List;
import java.util.Optional;

/** Use TestValuesCatalog to test partition push down and create function definition. */
public class TestValuesCatalog extends GenericInMemoryCatalog {
    private final boolean supportListPartitionByFilter;

    public TestValuesCatalog(
            String name, String defaultDatabase, boolean supportListPartitionByFilter) {
        super(name, defaultDatabase);
        this.supportListPartitionByFilter = supportListPartitionByFilter;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        if (!supportListPartitionByFilter) {
            throw new UnsupportedOperationException(
                    "TestValuesCatalog doesn't support list partition by filters");
        }

        return super.listPartitionsByFilter(tablePath, filters);
    }

    @Override
    public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return Optional.of(new TestFunctionDefinitionFactory());
    }
}
