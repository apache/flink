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

package org.apache.flink.table.sources.decorator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * CachedLookupableTableSource.
 * LIMITATION: now the function eval of the lookupTableSource implementation only supports parameter as Object or Object...
 * TODO: in the future, to extract the parameter type from the Method, but I think it's not much urgent.
 */
public class CachedLookupFunctionDecorator<T> extends TableFunction<T> {
	//default 1day.
	private static final long EXPIRED_TIME_MS_DEFAULT = 24 * 60 * 60 * 1000;
	private final TableFunction<T> lookupTableSource;
	private transient Cache<Row, List<T>> cache;
	private transient LookupFunctionInvoker.Evaluation realEval;
	private CollectorProxy collectorProxy;
	private final long expireTimeMS;
	private final long maximumSize;
	private final boolean recordStat;
	private final boolean isVariable;

	public CachedLookupFunctionDecorator(TableFunction<T> lookupTableSource, long maximumSize) {
		this(lookupTableSource, maximumSize, EXPIRED_TIME_MS_DEFAULT);
	}

	public CachedLookupFunctionDecorator(TableFunction<T> lookupTableSource, long maximumSize, long expireTimeMs) {
		this(lookupTableSource, maximumSize, expireTimeMs, true);
	}

	public CachedLookupFunctionDecorator(
		TableFunction<T> lookupTableSource, long maximumSize, long expireTimeMs, boolean recordStat) {
		this.lookupTableSource = lookupTableSource;
		this.maximumSize = maximumSize;
		this.expireTimeMS = expireTimeMs;
		this.recordStat = recordStat;
		this.isVariable = checkMethodVariable("eval", lookupTableSource.getClass());
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		lookupTableSource.open(context);
		collectorProxy = new CollectorProxy();
		lookupTableSource.setCollector(collectorProxy);
		LookupFunctionInvoker lookupFunctionInvoker = new LookupFunctionInvoker(lookupTableSource);
		realEval = lookupFunctionInvoker.getProxy();
		CacheBuilder cacheBuilder = CacheBuilder.newBuilder().expireAfterWrite(expireTimeMS,
			TimeUnit.MILLISECONDS).maximumSize(maximumSize);
		if (this.recordStat) {
			cacheBuilder.recordStats();
		}
		this.cache = cacheBuilder.build();
	}

	@Override
	public void close() throws Exception {
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
		lookupTableSource.close();
	}

	@Override
	public TypeInformation<T> getResultType() {
		return lookupTableSource.getResultType();
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return lookupTableSource.getParameterTypes(signature);
	}

	@VisibleForTesting
	Cache<Row, List<T>> getCache() {
		return cache;
	}

	public void eval(Object... keys) {
		try {
			Row keyRow = Row.of(keys);
			List<T> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				for (T cachedRow : cachedRows) {
					collect(cachedRow);
				}
			} else {
				collectorProxy.setCurrentKey(keyRow);
				if (isVariable) {
					//TODO: we can extract the exactly parameter In the future.
					realEval.eval(keys);
				} else {
					realEval.eval(keys[0]);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("CachedLookupTableSource exception:" + e.getCause(), e);
		}

	}

	private class CollectorProxy implements Collector<T> {

		private volatile Row currentKey;

		public void setCurrentKey(Row currentKey) {
			this.currentKey = currentKey;
		}

		@Override
		public void collect(T record) {
			try {
				List<T> cachedRows = cache.get(currentKey, ArrayList::new);
				cachedRows.add(record);
				CachedLookupFunctionDecorator.this.collect(record);
			} catch (Exception e) {
				throw new RuntimeException("CollectorProxy exception:" + e.getMessage(), e);
			}
		}

		@Override
		public void close() {

		}
	}

	private boolean checkMethodVariable(String methodName, Class clazz) {
		Method result = null;
		for (Method method : clazz.getMethods()) {
			if (methodName.equals(method.getName())) {
				if (result != null) {
					throw new RuntimeException("TableFunction only can have one eval function.");
				}
				result = method;
			}
		}
		if (result == null) {
			throw new RuntimeException("TableFunction must have eval function.");
		}
		return result.isVarArgs();
	}
}
