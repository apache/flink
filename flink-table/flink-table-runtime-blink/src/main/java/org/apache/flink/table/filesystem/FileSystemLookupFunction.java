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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Lookup table function for filesystem connector tables.
 */
public class FileSystemLookupFunction<T extends InputSplit> extends TableFunction<RowData> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemLookupFunction.class);

	// the max number of retries before throwing exception, in case of failure to load the table into cache
	private static final int MAX_RETRIES = 3;
	// interval between retries
	private static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

	private final InputFormat<RowData, T> inputFormat;
	// names and types of the records returned by the input format
	private final String[] producedNames;
	private final DataType[] producedTypes;
	private final Duration cacheTTL;

	// indices of lookup columns in the record returned by input format
	private final int[] lookupCols;
	// use Row as key for the cache
	private transient Map<Row, List<RowData>> cache;
	// timestamp when cache expires
	private transient long nextLoadTime;
	// serializer to copy RowData
	private transient TypeSerializer<RowData> serializer;
	// converters to convert data from internal to external in order to generate keys for the cache
	private final DataFormatConverter[] converters;

	public FileSystemLookupFunction(
			InputFormat<RowData, T> inputFormat,
			String[] lookupKeys,
			String[] producedNames,
			DataType[] producedTypes,
			Duration cacheTTL) {
		lookupCols = new int[lookupKeys.length];
		converters = new DataFormatConverter[lookupKeys.length];
		Map<String, Integer> nameToIndex = IntStream.range(0, producedNames.length).boxed().collect(
				Collectors.toMap(i -> producedNames[i], i -> i));
		for (int i = 0; i < lookupKeys.length; i++) {
			Integer index = nameToIndex.get(lookupKeys[i]);
			Preconditions.checkArgument(index != null, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
			converters[i] = DataFormatConverters.getConverterForDataType(producedTypes[index]);
			lookupCols[i] = index;
		}
		this.inputFormat = inputFormat;
		this.producedNames = producedNames;
		this.producedTypes = producedTypes;
		this.cacheTTL = cacheTTL;
	}

	@Override
	public TypeInformation<RowData> getResultType() {
		return InternalTypeInfo.ofFields(
				Arrays.stream(producedTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
				producedNames);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		cache = new HashMap<>();
		nextLoadTime = -1;
		// TODO: get ExecutionConfig from context?
		serializer = getResultType().createSerializer(new ExecutionConfig());
	}

	public void eval(Object... values) {
		Preconditions.checkArgument(values.length == lookupCols.length, "Number of values and lookup keys mismatch");
		checkCacheReload();
		for (int i = 0; i < values.length; i++) {
			values[i] = converters[i].toExternal(values[i]);
		}
		Row probeKey = Row.of(values);
		List<RowData> matchedRows = cache.get(probeKey);
		if (matchedRows != null) {
			for (RowData matchedRow : matchedRows) {
				collect(matchedRow);
			}
		}
	}

	@VisibleForTesting
	public Duration getCacheTTL() {
		return cacheTTL;
	}

	private void checkCacheReload() {
		if (nextLoadTime > System.currentTimeMillis()) {
			return;
		}
		if (nextLoadTime > 0) {
			LOG.info("Lookup join cache has expired after {} minute(s), reloading", getCacheTTL().toMinutes());
		} else {
			LOG.info("Populating lookup join cache");
		}
		int numRetry = 0;
		while (true) {
			cache.clear();
			try {
				T[] inputSplits = inputFormat.createInputSplits(1);
				GenericRowData reuse = new GenericRowData(producedNames.length);
				long count = 0;
				for (T split : inputSplits) {
					inputFormat.open(split);
					while (!inputFormat.reachedEnd()) {
						RowData row = inputFormat.nextRecord(reuse);
						count++;
						Row key = extractKey(row);
						List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
						rows.add(serializer.copy(row));
					}
					inputFormat.close();
				}
				nextLoadTime = System.currentTimeMillis() + getCacheTTL().toMillis();
				LOG.info("Loaded {} row(s) into lookup join cache", count);
				return;
			} catch (IOException e) {
				if (numRetry >= MAX_RETRIES) {
					throw new FlinkRuntimeException(
							String.format("Failed to load table into cache after %d retries", numRetry), e);
				}
				numRetry++;
				long toSleep = numRetry * RETRY_INTERVAL.toMillis();
				LOG.warn(String.format("Failed to load table into cache, will retry in %d seconds", toSleep / 1000), e);
				try {
					Thread.sleep(toSleep);
				} catch (InterruptedException ex) {
					LOG.warn("Interrupted while waiting to retry failed cache load, aborting");
					throw new FlinkRuntimeException(ex);
				}
			}
		}
	}

	private Row extractKey(RowData row) {
		Row key = new Row(lookupCols.length);
		for (int i = 0; i < lookupCols.length; i++) {
			key.setField(i, converters[i].toExternal(row, lookupCols[i]));
		}
		return key;
	}
}
