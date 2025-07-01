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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
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
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is a test catalog factory to create a catalog that does not extend {@link AbstractCatalog}.
 */
public class PlainTestCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "test-plain-catalog";

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
    public Catalog createCatalog(CatalogFactory.Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return new TestCatalog(context.getName(), helper.getOptions().get(DEFAULT_DATABASE));
    }

    /** Test catalog that does not extend {@link AbstractCatalog}. */
    public static class TestCatalog implements Catalog {
        private final GenericInMemoryCatalog innerCatalog;

        public TestCatalog(String name, String defaultDatabase) {
            this.innerCatalog = new GenericInMemoryCatalog(name, defaultDatabase);
        }

        @Override
        public void open() throws CatalogException {
            innerCatalog.open();
        }

        @Override
        public void close() throws CatalogException {
            innerCatalog.close();
        }

        @Override
        public String getDefaultDatabase() throws CatalogException {
            return innerCatalog.getDefaultDatabase();
        }

        @Override
        public List<String> listDatabases() throws CatalogException {
            return innerCatalog.listDatabases();
        }

        @Override
        public CatalogDatabase getDatabase(String databaseName)
                throws DatabaseNotExistException, CatalogException {
            return innerCatalog.getDatabase(databaseName);
        }

        @Override
        public boolean databaseExists(String databaseName) throws CatalogException {
            return innerCatalog.databaseExists(databaseName);
        }

        @Override
        public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
                throws DatabaseAlreadyExistException, CatalogException {
            innerCatalog.createDatabase(name, database, ignoreIfExists);
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
            innerCatalog.dropDatabase(name, cascade);
        }

        @Override
        public void alterDatabase(
                String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
                throws DatabaseNotExistException, CatalogException {
            innerCatalog.alterDatabase(name, newDatabase, ignoreIfNotExists);
        }

        @Override
        public List<String> listTables(String databaseName)
                throws DatabaseNotExistException, CatalogException {
            return innerCatalog.listTables(databaseName);
        }

        @Override
        public List<String> listViews(String databaseName)
                throws DatabaseNotExistException, CatalogException {
            return innerCatalog.listViews(databaseName);
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath)
                throws TableNotExistException, CatalogException {
            return innerCatalog.getTable(tablePath);
        }

        @Override
        public boolean tableExists(ObjectPath tablePath) throws CatalogException {
            return innerCatalog.tableExists(tablePath);
        }

        @Override
        public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            innerCatalog.dropTable(tablePath, ignoreIfNotExists);
        }

        @Override
        public void renameTable(
                ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
                throws TableNotExistException, TableAlreadyExistException, CatalogException {
            innerCatalog.renameTable(tablePath, newTableName, ignoreIfNotExists);
        }

        @Override
        public void createTable(
                ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
                throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
            innerCatalog.createTable(tablePath, table, ignoreIfExists);
        }

        @Override
        public void alterTable(
                ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            innerCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);
        }

        @Override
        public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
                throws TableNotExistException, TableNotPartitionedException, CatalogException {
            return innerCatalog.listPartitions(tablePath);
        }

        @Override
        public List<CatalogPartitionSpec> listPartitions(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws TableNotExistException,
                        TableNotPartitionedException,
                        PartitionSpecInvalidException,
                        CatalogException {
            return innerCatalog.listPartitions(tablePath, partitionSpec);
        }

        @Override
        public List<CatalogPartitionSpec> listPartitionsByFilter(
                ObjectPath tablePath, List<Expression> filters)
                throws TableNotExistException, TableNotPartitionedException, CatalogException {
            return innerCatalog.listPartitionsByFilter(tablePath, filters);
        }

        @Override
        public CatalogPartition getPartition(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws PartitionNotExistException, CatalogException {
            return innerCatalog.getPartition(tablePath, partitionSpec);
        }

        @Override
        public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws CatalogException {
            return innerCatalog.partitionExists(tablePath, partitionSpec);
        }

        @Override
        public void createPartition(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogPartition partition,
                boolean ignoreIfExists)
                throws TableNotExistException,
                        TableNotPartitionedException,
                        PartitionSpecInvalidException,
                        PartitionAlreadyExistsException,
                        CatalogException {
            innerCatalog.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
        }

        @Override
        public void dropPartition(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            innerCatalog.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
        }

        @Override
        public void alterPartition(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogPartition newPartition,
                boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            innerCatalog.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
        }

        @Override
        public List<String> listFunctions(String dbName)
                throws DatabaseNotExistException, CatalogException {
            return innerCatalog.listFunctions(dbName);
        }

        @Override
        public CatalogFunction getFunction(ObjectPath functionPath)
                throws FunctionNotExistException, CatalogException {
            return innerCatalog.getFunction(functionPath);
        }

        @Override
        public boolean functionExists(ObjectPath functionPath) throws CatalogException {
            return innerCatalog.functionExists(functionPath);
        }

        @Override
        public void createFunction(
                ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
                throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
            innerCatalog.createFunction(functionPath, function, ignoreIfExists);
        }

        @Override
        public void alterFunction(
                ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
                throws FunctionNotExistException, CatalogException {
            innerCatalog.alterFunction(functionPath, newFunction, ignoreIfNotExists);
        }

        @Override
        public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
                throws FunctionNotExistException, CatalogException {
            innerCatalog.dropFunction(functionPath, ignoreIfNotExists);
        }

        @Override
        public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
                throws TableNotExistException, CatalogException {
            return innerCatalog.getTableStatistics(tablePath);
        }

        @Override
        public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
                throws TableNotExistException, CatalogException {
            return innerCatalog.getTableColumnStatistics(tablePath);
        }

        @Override
        public CatalogTableStatistics getPartitionStatistics(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws PartitionNotExistException, CatalogException {
            return innerCatalog.getPartitionStatistics(tablePath, partitionSpec);
        }

        @Override
        public CatalogColumnStatistics getPartitionColumnStatistics(
                ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
                throws PartitionNotExistException, CatalogException {
            return innerCatalog.getPartitionColumnStatistics(tablePath, partitionSpec);
        }

        @Override
        public void alterTableStatistics(
                ObjectPath tablePath,
                CatalogTableStatistics tableStatistics,
                boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            innerCatalog.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
        }

        @Override
        public void alterTableColumnStatistics(
                ObjectPath tablePath,
                CatalogColumnStatistics columnStatistics,
                boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            innerCatalog.alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists);
        }

        @Override
        public void alterPartitionStatistics(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogTableStatistics partitionStatistics,
                boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            innerCatalog.alterPartitionStatistics(
                    tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists);
        }

        @Override
        public void alterPartitionColumnStatistics(
                ObjectPath tablePath,
                CatalogPartitionSpec partitionSpec,
                CatalogColumnStatistics columnStatistics,
                boolean ignoreIfNotExists)
                throws PartitionNotExistException, CatalogException {
            innerCatalog.alterPartitionColumnStatistics(
                    tablePath, partitionSpec, columnStatistics, ignoreIfNotExists);
        }
    }
}
