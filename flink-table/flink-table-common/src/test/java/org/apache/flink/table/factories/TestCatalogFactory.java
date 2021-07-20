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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Test catalog factory for catalog discovery tests. */
public class TestCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "test-catalog";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        return new TestCatalog(context.getName(), context.getOptions());
    }

    /** Test catalog for discovery testing. */
    public static class TestCatalog implements Catalog {
        private final String name;
        private final Map<String, String> options;

        public TestCatalog(String name, Map<String, String> options) {
            this.name = name;
            this.options = options;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        @Override
        public void open() throws CatalogException {}

        @Override
        public void close() throws CatalogException {}

        @Override
        public String getDefaultDatabase() throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listDatabases() throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogDatabase getDatabase(String databaseName)
                throws DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean databaseExists(String databaseName) throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
                throws DatabaseAlreadyExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterDatabase(
                String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
                throws DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String databaseName)
                throws DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listViews(String databaseName)
                throws DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(ObjectPath tablePath) throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(
                ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
                throws TableNotExistException, TableAlreadyExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(
                ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
                throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterTable(
                ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
                throws TableNotExistException, TableNotPartitionedException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<CatalogPartitionSpec> listPartitions(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws TableNotExistException, TableNotPartitionedException,
                        PartitionSpecInvalidException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<CatalogPartitionSpec> listPartitionsByFilter(
                ObjectPath tablePath, List<Expression> filters)
                throws TableNotExistException, TableNotPartitionedException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogPartition getPartition(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createPartition(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogPartition partition,
                boolean ignoreIfExists)
                throws TableNotExistException, TableNotPartitionedException,
                        PartitionSpecInvalidException, PartitionAlreadyExistsException,
                        CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropPartition(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterPartition(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogPartition newPartition,
                boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listFunctions(String dbName)
                throws DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogFunction getFunction(ObjectPath functionPath)
                throws FunctionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean functionExists(ObjectPath functionPath) throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createFunction(
                ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
                throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterFunction(
                ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
                throws FunctionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
                throws FunctionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogTableStatistics getPartitionStatistics(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogColumnStatistics getPartitionColumnStatistics(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterTableStatistics(
                ObjectPath tablePath,
                CatalogTableStatistics tableStatistics,
                boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterTableColumnStatistics(
                ObjectPath tablePath,
                CatalogColumnStatistics columnStatistics,
                boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException, TablePartitionedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterPartitionStatistics(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogTableStatistics partitionStatistics,
                boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterPartitionColumnStatistics(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogColumnStatistics columnStatistics,
                boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            throw new UnsupportedOperationException();
        }
    }
}
