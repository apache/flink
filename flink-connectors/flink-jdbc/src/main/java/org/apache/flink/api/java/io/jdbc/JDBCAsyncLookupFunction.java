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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.getFieldFromResultSet;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link AsyncTableFunction} used in temporal table join as an async lookup function.
 *
 * <p>Support batch query to avoid frequent accessing to remote databases.
 * 1.The batch querySize default 1.
 *
 * <p>Support setting connection pool size to avoid frequent creating and destroying connection.
 * or avoid creating unlimited connections.
 * 1.The connection pool size default 10.
 *
 * <p>Support setting thread pool size to avoid frequent creating and destroying thread.
 * 1.The thread pool size default 10.
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class JDBCAsyncLookupFunction extends AsyncTableFunction<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCAsyncLookupFunction.class);
	private static final long serialVersionUID = 1L;
	private int currentQuerySize = 0;

	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;
	private final TypeInformation[] keyTypes;
	private final int[] keySqlTypes;
	private final int[] keyNameIndex;
	private final int[] outputSqlTypes;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final int batchQuerySize;
	private final int threadPoolSize;

	private ConnectionManager connectionManager;
	private transient ExecutorService executorService;
	private transient Cache<Row, List<Row>> cache;
	private transient Map<Row, List<CompletableFuture<Collection<Row>>>> batchQueryMap;

	public JDBCAsyncLookupFunction(
		JDBCOptions options, JDBCLookupOptions lookupOptions,
		String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames) {

		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyNames)
			.map(s -> {
				checkArgument(nameList.contains(s),
					"keyName %s can't find in fieldNames %s.", s, nameList);
				return fieldTypes[nameList.indexOf(s)];
			})
			.toArray(TypeInformation[]::new);
		this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.keyNameIndex = Arrays.stream(keyNames).mapToInt(key -> nameList.indexOf(key)).toArray();
		this.outputSqlTypes = Arrays.stream(fieldTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
		this.batchQuerySize = lookupOptions.getBatchQuerySize();
		this.threadPoolSize = lookupOptions.getThreadPoolSize();
		String query = options.getDialect().getBatchSelectFromStatement(
				options.getTableName(), fieldNames, keyNames, batchQuerySize);
		connectionManager = new ConnectionManager(
			lookupOptions.getMaxPoolSize(), options.getDriverName(),
			options.getDbURL(), options.getUsername(), options.getPassword(), query);
	}

	@Override
	public void open(FunctionContext context) throws Exception {

		connectionManager.initPool();
		// Create lru memory cache
		this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
			.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
			.maximumSize(cacheMaxSize)
			.build();
		// Create async lookup thread pool
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
			.setDaemon(true)
			.setNameFormat("Flink JDBCAsyncLookupFunction Thread %d")
			.build();
		executorService = Executors.newFixedThreadPool(threadPoolSize, threadFactory);
		// Create global batch query map
		if (batchQueryMap == null) {
			batchQueryMap = new HashMap<>();
		}
	}

	public void eval(CompletableFuture<Collection<Row>> result, Object... keys) {
		Row keyRow = Row.of(keys);
		if (cache != null) {
			List<Row> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				result.complete(cachedRows);
				return;
			}
		}

		List resultFutureList = batchQueryMap.get(keyRow);
		if (null == resultFutureList) {
			List resultList = new ArrayList();
			resultList.add(result);
			batchQueryMap.put(keyRow, resultList);
		} else {
			resultFutureList.add(result);
		}
		currentQuerySize++;
		if (currentQuerySize < batchQuerySize) {
			return;
		}

		Map<Row, List<CompletableFuture<Collection<Row>>>> localQueryMap = new HashMap<>();
		localQueryMap.putAll(batchQueryMap);
		currentQuerySize = 0;
		batchQueryMap.clear();

		CompletableFuture.runAsync(new Runnable() {
			@Override
			public void run() {
				JDBCConnection jdbcConnection = connectionManager.acquireJDBCConnection();
				try {
					executeAsyncLookup(localQueryMap, jdbcConnection);
				} finally {
					connectionManager.releaseJDBCConnection(jdbcConnection);
				}
			}
		}, executorService);
	}

	/**
	 * Execute async query for JDBCAsyncLookupFunction.
	 * @param keysMap Map to store lookup Keys and corresponding future.
	 * @param jdbcConnection JDBCConnection accesses to remote databases.
	 */
	private void executeAsyncLookup(
		Map<Row, List<CompletableFuture<Collection<Row>>>> keysMap, JDBCConnection jdbcConnection) {

		PreparedStatement statement = jdbcConnection.getStatement();
		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try {
				// Set statement
				int keysMapIndex = 0;
				for (Row keys : keysMap.keySet()) {
					for (int i = 0; i < keysMap.get(keys).size(); i++) {
						for (int j = 0; j < keys.getArity(); j++) {
							JDBCUtils.setField(statement, keySqlTypes[j], keys.getField(j),
								keysMapIndex * keys.getArity() + j);
						}
						keysMapIndex++;
					}
				}
				// Get data from resultSet
				Map<Row, List<Row>> resultData = new HashMap<>();
				try (ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						Row value = new Row(outputSqlTypes.length);
						for (int i = 0; i < outputSqlTypes.length; i++) {
							value.setField(i, getFieldFromResultSet(i, outputSqlTypes[i], resultSet));
						}
						Row key = Row.project(value, keyNameIndex);
						if (null == resultData.get(key)) {
							List<Row> rows = new ArrayList<>();
							rows.add(value);
							resultData.put(key, rows);
						} else {
							List<Row> rows = resultData.get(key);
							rows.add(value);
						}
					}
				}
				// Return data by called future#complete
				for (Map.Entry entry : keysMap.entrySet()) {
					List<CompletableFuture<Collection<Row>>> futures =
						(List<CompletableFuture<Collection<Row>>>) entry.getValue();

					futures.stream().forEach(future -> {
						List<Row> rows = resultData.get(entry.getKey());
						if (null == rows) {
							future.complete(Collections.emptyList());
						} else {
							if (cache != null) {
								cache.put((Row) entry.getKey(), rows);
							}
							future.complete(rows);
						}
					});
				}
				break;
			} catch (SQLException e) {
				if (retry >= maxRetryTimes) {
					for (Map.Entry entry : keysMap.entrySet()) {
						List<CompletableFuture<Collection<Row>>> futures =
							(List<CompletableFuture<Collection<Row>>>) entry.getValue();
						futures.stream().forEach(future -> {
							future.completeExceptionally(new RuntimeException("Execution of JDBC statement failed.", e));
						});
					}
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	public static JDBCLookupBuilder builder() {
		return new JDBCLookupBuilder();
	}

	/**
	 * Close resources.
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
		if (executorService != null) {
			executorService.shutdown();
		}
		if (connectionManager != null) {
			try {
				connectionManager.close();
			} catch (SQLException e) {
				LOG.info("JDBC could not be closed: " + e.getMessage());
			}
		}
	}

}
