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

package org.apache.flink.table.catalog;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.exceptions.CatalogEntryNotExistException;
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
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.UncheckedExecutionException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader.asyncReloading;
import static org.apache.flink.util.ExceptionUtils.throwAnotherIfInstanceOf;
import static org.apache.flink.util.ExceptionUtils.throwIfInstanceOf;
import static org.apache.flink.util.ExceptionUtils.throwIfUnchecked;

/**
 * A thread-safe cached catalog which can delegate all of {@code AbstractCatalog} implementation.
 *
 * <pre>
 * delegate pattern:
 *   all read methods:
 *       1. get from cache firstly.
 *       2. the private method beginning with `load` prefix is used for
 *          reloading cache with delegate.
 *   all write methods:
 *       1. just proxy to delegate directly.
 *       2. update related cache.
 * </pre>
 */
public class GenericCachedCatalog extends AbstractCatalog  {

	private final AbstractCatalog delegate;

	private final boolean asyncReloadEnabled;

	private final int executorSize;

	private final long expiresAfterWriteMillis;

	private final long refreshMills;

	private final long maximumSize;

	// --- general schema cache ---
	// K: CATALOG_NAME, V: databaseName list
	private final LoadingCache<String, List<String>> databaseNamesCache;
	// K: databaseName, V: CatalogDatabase
	private final LoadingCache<String, CatalogDatabase> databaseCache;
	// K: databaseName, V: tableName list
	private final LoadingCache<String, List<String>> tableNamesCache;
	// K: databaseName + tableName, V: CatalogBaseTable
	private final LoadingCache<ObjectPath, CatalogBaseTable> tableCache;
	// K: databaseName, V: viewName list
	private final LoadingCache<String, List<String>> viewNamesCache;
	// K: databaseName + tableName, V: CatalogPartitionSpec list
	private final LoadingCache<ObjectPath, List<CatalogPartitionSpec>> partitionSpecsCache;
	// K: databaseName + tableName + filter expression, V: CatalogPartitionSpec list
	private final LoadingCache<Tuple2<ObjectPath, List<Expression>>,
		List<CatalogPartitionSpec>> partitionSpecsFilterCache;
	// K: databaseName + tableName + partitionSpec, V: CatalogPartition
	private final LoadingCache<Tuple2<ObjectPath, CatalogPartitionSpec>, CatalogPartition>
		partitionCache;
	// K: databaseName, V: function name list
	private final LoadingCache<String, List<String>> functionNamesCache;
	// K: databaseName + function name, V: CatalogFunction
	private final LoadingCache<ObjectPath, CatalogFunction> functionCache;

	// --- statistic cache ---
	// K: databaseName + tableName, V: CatalogTableStatistics
	private final LoadingCache<ObjectPath, CatalogTableStatistics> tableStatisticsCache;
	// K: databaseName + tabletName, V: CatalogColumnStatistics
	private final LoadingCache<ObjectPath, CatalogColumnStatistics> columnStatisticsCache;
	// K: databaseName + tabletName + partitionSpec, V: CatalogTableStatistics
	private final LoadingCache<Tuple2<ObjectPath, CatalogPartitionSpec>, CatalogTableStatistics>
		partitionStatisticsCache;
	// K: databaseName + tableName + partitionSpec, V: CatalogTableStatistics
	private final LoadingCache<Tuple2<ObjectPath, CatalogPartitionSpec>, CatalogColumnStatistics>
		partitionColumnStatisticsCache;

	public GenericCachedCatalog(
			AbstractCatalog delegateCatalog,
			String catalogName,
			String database,
			boolean asyncReloadEnabled,
			int executorSize,
			Duration cacheTtl,
			Duration refreshInterval,
			long maximumSize) {
		this(
			delegateCatalog,
			catalogName,
			database,
			asyncReloadEnabled,
			executorSize,
			cacheTtl.toMillis(),
			refreshInterval.toMillis() >= cacheTtl.toMillis() ? 0L : refreshInterval.toMillis(),
			maximumSize);
	}

	private GenericCachedCatalog(
			AbstractCatalog delegateCatalog,
			String catalogName,
			String database,
			boolean asyncReloadEnabled,
			int executorSize,
			long expiresAfterWriteMillis,
			long refreshMills,
			long maximumSize) {
		super(catalogName, database);
		Preconditions.checkNotNull(delegateCatalog, "catalog delegate can not be null");
		this.delegate = delegateCatalog;
		this.asyncReloadEnabled = asyncReloadEnabled;
		this.executorSize = executorSize;
		this.expiresAfterWriteMillis = expiresAfterWriteMillis;
		this.refreshMills = refreshMills;
		this.maximumSize = maximumSize;

		databaseNamesCache = buildCache(this::loadDatabases);

		databaseCache = buildCache(this::loadDatabase);

		tableNamesCache = buildCache(this::loadTables);

		tableCache = buildCache(this::loadTable);

		viewNamesCache = buildCache(this::loadViews);

		partitionSpecsCache = buildCache(this::loadPartitions);

		partitionSpecsFilterCache = buildCache(this::loadPartitionsByFilter);

		partitionCache = buildCache(this::loadPartition);

		functionNamesCache = buildCache(this::loadFunctions);

		functionCache = buildCache(this::loadFunction);

		tableStatisticsCache = buildCache(this::loadTableStatistics);

		columnStatisticsCache = buildCache(this::loadTableColumnStatistics);

		partitionStatisticsCache = buildCache(this::loadPartitionStatistics);

		partitionColumnStatisticsCache = buildCache(this::loadPartitionColumnStatistics);
	}

	private <K, V, E extends Exception> LoadingCache<K, V> buildCache(
			FunctionWithException<K, V, E> loadFunction) {
		CacheBuilder cacheBuilder = newCacheBuilder(expiresAfterWriteMillis, refreshMills,
			maximumSize);
		CacheLoader cacheLoader = new CacheLoader<K, V> (){
			@Override
			public V load(K key) throws E, CatalogEntryNotExistException {
				V value = loadFunction.apply(key);
				if (value == null) {
					throw new CatalogEntryNotExistException(delegate.getName(), key);
				}
				return value;
			}
		};
		if (asyncReloadEnabled) {
			ExecutorService executor = Executors.newFixedThreadPool(executorSize);
			return cacheBuilder.build(asyncReloading(cacheLoader, executor));
		} else {
			return cacheBuilder.build(cacheLoader);
		}
	}

	private <K, V, E extends Exception> boolean checkExist(
			LoadingCache<K, V> cache,
			K key,
			Class<E> noExistingExceptionClazz) throws CatalogException {
		try {
			return cache.get(key) != null;
		} catch (ExecutionException | UncheckedExecutionException e) {
			if (noExistingExceptionClazz.isInstance(e.getCause()) ||
				e.getCause() instanceof CatalogEntryNotExistException) {
				return false;
			}
			throw new CatalogException(e);
		}
	}

	private void invalidateAllCache() {
		databaseNamesCache.invalidateAll();
		databaseCache.invalidateAll();
		tableNamesCache.invalidateAll();
		tableCache.invalidateAll();
		viewNamesCache.invalidateAll();
		partitionSpecsCache.invalidateAll();
		partitionSpecsFilterCache.invalidateAll();
		partitionCache.invalidateAll();
		functionNamesCache.invalidateAll();
		functionCache.invalidateAll();
		tableStatisticsCache.invalidateAll();
		columnStatisticsCache.invalidateAll();
		partitionStatisticsCache.invalidateAll();
		partitionColumnStatisticsCache.invalidateAll();
	}

	@Override
	public Optional<Factory> getFactory() {
		return delegate.getFactory();
	}

	@Override
	public Optional<TableFactory> getTableFactory() {
		return delegate.getTableFactory();
	}

	@Override
	public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
		return delegate.getFunctionDefinitionFactory();
	}

	@Override
	public void open() throws CatalogException {
		delegate.open();
	}

	@Override
	public void close() throws CatalogException {
		delegate.close();
		invalidateAllCache();
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		try {
			return databaseNamesCache.get(delegate.getName());
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private List<String> loadDatabases(String catalogName) throws CatalogException {
		return delegate.listDatabases();
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		try {
			return databaseCache.get(databaseName);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), DatabaseNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				DatabaseNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogDatabase loadDatabase(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		return delegate.getDatabase(databaseName);
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return checkExist(databaseCache, databaseName, DatabaseNotExistException.class);
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
			throws DatabaseAlreadyExistException, CatalogException {
		try {
			delegate.createDatabase(name, database, ignoreIfExists);
		} finally {
			invalidateDatabaseCache(name);
		}
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
			throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		try {
			delegate.dropDatabase(name, ignoreIfNotExists);
		} finally {
			invalidateDatabaseCache(name);
		}
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
			throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		try {
			delegate.dropDatabase(name, ignoreIfNotExists, cascade);
		} finally {
			invalidateDatabaseCache(name);
		}
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
			throws DatabaseNotExistException, CatalogException {
		try {
			delegate.alterDatabase(name, newDatabase, ignoreIfNotExists);
		} finally {
			invalidateDatabaseCache(name);
		}
	}

	@Override
	public List<String> listTables(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		try {
			return tableNamesCache.get(databaseName);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), DatabaseNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				DatabaseNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private List<String> loadTables(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		return delegate.listTables(databaseName);
	}

	@Override
	public List<String> listViews(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		try {
			return viewNamesCache.get(databaseName);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), DatabaseNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				DatabaseNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private List<String> loadViews(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		return delegate.listViews(databaseName);
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		try {
			return tableCache.get(tablePath);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TableNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				TableNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogBaseTable loadTable(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		return delegate.getTable(tablePath);
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return checkExist(tableCache, tablePath, TableNotExistException.class);
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		try {
			delegate.dropTable(tablePath, ignoreIfNotExists);
		} finally {
			invalidateTableCache(tablePath);
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
			throws TableNotExistException, TableAlreadyExistException, CatalogException {
		try {
			delegate.renameTable(tablePath, newTableName, ignoreIfNotExists);
		} finally {
			invalidateTableCache(tablePath);
		}
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
			throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		try {
			delegate.createTable(tablePath, table, ignoreIfExists);
		} finally {
			invalidateTableCache(tablePath);
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		try {
			delegate.alterTable(tablePath, newTable, ignoreIfNotExists);
		} finally {
			invalidateTableCache(tablePath);
		}
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		try {
			return partitionSpecsCache.get(tablePath);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TableNotExistException.class);
			throwIfInstanceOf(e.getCause(), TableNotPartitionedException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				TableNotPartitionedException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private List<CatalogPartitionSpec> loadPartitions(ObjectPath tablePath)
			throws TableNotPartitionedException, TableNotExistException {
		return delegate.listPartitions(tablePath);
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws TableNotExistException,
			TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
		return delegate.listPartitions(tablePath, partitionSpec);
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(
			ObjectPath tablePath,
			List<Expression> filters)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		try {
			return partitionSpecsFilterCache.get(new Tuple2<>(tablePath, filters));
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TableNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				TableNotPartitionedException.class);
			throwIfInstanceOf(e.getCause(), TableNotPartitionedException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private List<CatalogPartitionSpec> loadPartitionsByFilter(
			Tuple2<ObjectPath, List<Expression>> key)
			throws TableNotPartitionedException, TableNotExistException {
		return delegate.listPartitionsByFilter(key.f0, key.f1);
	}

	@Override
	public CatalogPartition getPartition(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		try {
			return partitionCache.get(new Tuple2<>(tablePath, partitionSpec));
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), PartitionNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				PartitionNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogPartition loadPartition(Tuple2<ObjectPath, CatalogPartitionSpec> key)
			throws PartitionNotExistException, CatalogException {
		return delegate.getPartition(key.f0, key.f1);
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		return checkExist(
			partitionCache, new Tuple2<>(tablePath, partitionSpec), PartitionNotExistException.class);
	}

	@Override
	public void createPartition(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec,
			CatalogPartition partition,
			boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException,
			PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		try {
			delegate.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
		} finally {
			invalidatePartitionCache(tablePath, partitionSpec);
		}
	}

	@Override
	public void dropPartition(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec,
			boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		try {
			delegate.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
		} finally {
			invalidatePartitionCache(tablePath, partitionSpec);
		}
	}

	@Override
	public void alterPartition(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec,
			CatalogPartition newPartition,
			boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		try {
			delegate.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
		} finally {
			invalidatePartitionCache(tablePath, partitionSpec);
		}
	}

	@Override
	public List<String> listFunctions(String dbName)
			throws DatabaseNotExistException, CatalogException {
		try {
			return functionNamesCache.get(dbName);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), DatabaseNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				DatabaseNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private List<String> loadFunctions(String dbName)
			throws DatabaseNotExistException, CatalogException {
		return delegate.listFunctions(dbName);
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath)
			throws FunctionNotExistException, CatalogException {
		try {
			return functionCache.get(functionPath);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), FunctionNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				FunctionNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogFunction loadFunction(ObjectPath functionPath)
			throws FunctionNotExistException, CatalogException {
		return delegate.getFunction(functionPath);
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		return checkExist(functionCache, functionPath, FunctionNotExistException.class);
	}

	@Override
	public void createFunction(
			ObjectPath functionPath,
			CatalogFunction function,
			boolean ignoreIfExists)
			throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
		try {
			delegate.createFunction(functionPath, function, ignoreIfExists);
		} finally {
			invalidateFunctionCache(functionPath);
		}
	}

	@Override
	public void alterFunction(
			ObjectPath functionPath,
			CatalogFunction newFunction,
			boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
		try {
			delegate.alterFunction(functionPath, newFunction, ignoreIfNotExists);
		} finally {
			functionCache.invalidate(functionPath);
		}
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
			throws FunctionNotExistException, CatalogException {
		try {
			delegate.dropFunction(functionPath, ignoreIfNotExists);
		} finally {
			invalidateFunctionCache(functionPath);
		}
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		try {
			return tableStatisticsCache.get(tablePath);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TableNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				TableNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogTableStatistics loadTableStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		return delegate.getTableStatistics(tablePath);
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		try {
			return columnStatisticsCache.get(tablePath);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TableNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				TableNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogColumnStatistics loadTableColumnStatistics(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		return delegate.getTableColumnStatistics(tablePath);
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		try {
			return partitionStatisticsCache.get(new Tuple2<>(tablePath, partitionSpec));
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), PartitionNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				PartitionNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogTableStatistics loadPartitionStatistics(
			Tuple2<ObjectPath, CatalogPartitionSpec> key)
			throws PartitionNotExistException, CatalogException {
		return delegate.getPartitionStatistics(key.f0, key.f1);
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		try {
			return partitionColumnStatisticsCache.get(new Tuple2<>(tablePath, partitionSpec));
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), PartitionNotExistException.class);
			throwAnotherIfInstanceOf(e.getCause(), CatalogEntryNotExistException.class,
				PartitionNotExistException.class);
			throwIfInstanceOf(e.getCause(), CatalogException.class);
			throwIfUnchecked(e);
			throw new CatalogException(e);
		}
	}

	private CatalogColumnStatistics loadPartitionColumnStatistics(
			Tuple2<ObjectPath, CatalogPartitionSpec> key)
			throws PartitionNotExistException, CatalogException {
		return delegate.getPartitionColumnStatistics(key.f0, key.f1);
	}

	@Override
	public void alterTableStatistics(
			ObjectPath tablePath,
			CatalogTableStatistics tableStatistics,
			boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
		try {
			delegate.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
		} finally {
			tableStatisticsCache.invalidate(tablePath);
		}
	}

	@Override
	public void alterTableColumnStatistics(
			ObjectPath tablePath,
			CatalogColumnStatistics columnStatistics,
			boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException, TablePartitionedException {
		try {
			delegate.alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists);
		} finally {
			columnStatisticsCache.invalidate(tablePath);
		}
	}

	@Override
	public void alterPartitionStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec,
			CatalogTableStatistics partitionStatistics,
			boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		try {
			delegate.alterPartitionStatistics(
				tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists);
		} finally {
			partitionStatisticsCache.invalidate(new Tuple2<>(tablePath, partitionSpec));
		}
	}

	@Override
	public void alterPartitionColumnStatistics(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec,
			CatalogColumnStatistics columnStatistics,
			boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		try {
			delegate.alterPartitionColumnStatistics(
				tablePath, partitionSpec, columnStatistics, ignoreIfNotExists);
		} finally {
			partitionColumnStatisticsCache.invalidate(new Tuple2<>(tablePath, partitionSpec));
		}
	}

	protected void invalidateDatabaseCache(String dbName) {
		databaseNamesCache.invalidate(delegate.getName());
		databaseCache.invalidate(dbName);
		tableNamesCache.invalidate(dbName);
		tableCache.asMap().keySet().stream()
			.filter(key -> key.getDatabaseName().equals(dbName))
			.forEach(tableCache::invalidate);
		viewNamesCache.asMap().keySet().stream()
			.filter(key -> key.equals(dbName))
			.forEach(viewNamesCache::invalidate);
		partitionSpecsCache.asMap().keySet().stream()
			.filter(key -> key.getDatabaseName().equals(dbName))
			.forEach(partitionSpecsCache::invalidate);
		partitionSpecsFilterCache.asMap().keySet().stream()
			.filter(key -> key.f0.getDatabaseName().equals(dbName))
			.forEach(partitionSpecsFilterCache::invalidate);
		partitionCache.asMap().keySet().stream()
			.filter(key -> key.f0.getDatabaseName().equals(dbName))
			.forEach(partitionCache::invalidate);
		functionNamesCache.asMap().keySet().stream()
			.filter(key -> key.equals(dbName))
			.forEach(functionNamesCache::invalidate);
		functionCache.asMap().keySet().stream()
			.filter(key -> key.getDatabaseName().equals(dbName))
			.forEach(functionCache::invalidate);

		tableStatisticsCache.asMap().keySet().stream()
			.filter(key -> key.getDatabaseName().equals(dbName))
			.forEach(tableStatisticsCache::invalidate);
		columnStatisticsCache.asMap().keySet().stream()
			.filter(key -> key.getDatabaseName().equals(dbName))
			.forEach(columnStatisticsCache::invalidate);
		partitionStatisticsCache.asMap().keySet().stream()
			.filter(key -> key.f0.getDatabaseName().equals(dbName))
			.forEach(partitionStatisticsCache::invalidate);
		partitionColumnStatisticsCache.asMap().keySet().stream()
			.filter(key -> key.f0.getDatabaseName().equals(dbName))
			.forEach(partitionStatisticsCache::invalidate);
	}

	protected void invalidateTableCache(ObjectPath tablePath) {
		tableNamesCache.invalidate(tablePath.getDatabaseName());
		tableCache.invalidate(tablePath);
		partitionSpecsCache.asMap().keySet().stream()
			.filter(key -> key.equals(tablePath))
			.forEach(partitionSpecsCache::invalidate);
		partitionSpecsFilterCache.asMap().keySet().stream()
			.filter(key -> key.f0.equals(tablePath))
			.forEach(partitionSpecsFilterCache::invalidate);
		partitionCache.asMap().keySet().stream()
			.filter(key -> key.f0.equals(tablePath))
			.forEach(partitionCache::invalidate);

		tableStatisticsCache.invalidate(tablePath);
		columnStatisticsCache.invalidate(tablePath);
		partitionStatisticsCache.asMap().keySet().stream()
			.filter(key -> key.f0.equals(tablePath))
			.forEach(partitionStatisticsCache::invalidate);
		partitionColumnStatisticsCache.asMap().keySet().stream()
			.filter(key -> key.f0.equals(tablePath))
			.forEach(partitionColumnStatisticsCache::invalidate);
	}

	protected void invalidatePartitionCache(
			ObjectPath tablePath,
			CatalogPartitionSpec partitionSpec) {
		partitionSpecsCache.invalidate(tablePath);
		partitionSpecsFilterCache.invalidateAll();
		partitionCache.invalidate(new Tuple2<>(tablePath, partitionSpec));

		tableStatisticsCache.invalidate(tablePath);
		columnStatisticsCache.invalidate(tablePath);
		partitionStatisticsCache.invalidate(new Tuple2<>(tablePath, partitionSpec));
		partitionColumnStatisticsCache.invalidate(new Tuple2<>(tablePath, partitionSpec));
	}

	protected void invalidateFunctionCache(ObjectPath functionPath) {
		functionNamesCache.invalidate(functionPath.getDatabaseName());
		functionCache.invalidate(functionPath);
	}

	private static CacheBuilder<Object, Object> newCacheBuilder(
			long expiresAfterWriteMillis,
			long refreshMillis,
			long maximumSize) {
		CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
		if (expiresAfterWriteMillis >= 0) {
			cacheBuilder.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS);
		}
		if (refreshMillis > 0 && (expiresAfterWriteMillis <= 0 ||
			expiresAfterWriteMillis > refreshMillis)) {
			cacheBuilder.refreshAfterWrite(refreshMillis, MILLISECONDS);
		}
		cacheBuilder.maximumSize(maximumSize);
		return cacheBuilder;
	}
}
