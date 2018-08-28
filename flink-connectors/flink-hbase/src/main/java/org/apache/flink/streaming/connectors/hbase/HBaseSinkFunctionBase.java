/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.hbase.util.HBaseUtils;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HBaseSinkFunctionBase is the common abstract class of HBaseSinkFunction.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class HBaseSinkFunctionBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

	private static final Logger log = LoggerFactory.getLogger(HBaseSinkFunctionBase.class);

	// ------------------------------------------------------------------------
	//  Internal bulk processor configuration
	// ------------------------------------------------------------------------

	public static final String CONFIG_KEY_BATCH_FLUSH_ENABLE = "connector.batch-flush.enable";
	public static final String CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS = "connector.batch-flush.max-mutations";
	public static final String CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB = "connector.batch-flush.max-size.mb";
	public static final String CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS = "connector.batch-flush.interval.ms";

	protected final int rowKeyIndex;
	protected final String[] fieldNames;
	protected final TypeInformation<?>[] fieldTypes;
	protected final String[] columnFamilies;
	protected final String[] qualifiers;
	protected final int[] fieldElementIndexMapping;

	private final HBaseTableBuilder tableBuilder;
	/** The timer that triggers periodic flush to HBase. */
	private transient ScheduledThreadPoolExecutor executor;

	/** The lock to safeguard the flush commits. */
	private transient Object lock;

	private transient Connection connection;
	private transient Table table;

	private List<Mutation> mutaionBuffer = new LinkedList<>();
	private long estimateSize = 0;

	private final boolean batchFlushEnable;
	private final long batchFlushMaxMutations;
	private final long batchFlushMaxSizeInBits;
	private final long batchFlushIntervalMillis;

	private boolean isRunning = false;

	public HBaseSinkFunctionBase(
		HBaseTableBuilder tableBuilder,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] outputFieldNames,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes){
		this.tableBuilder = tableBuilder;
		this.rowKeyIndex = rowKeyIndex;
		this.fieldNames = fieldNames;
		this.columnFamilies = columnFamilies;
		this.qualifiers = qualifiers;
		this.fieldTypes = fieldTypes;
		this.fieldElementIndexMapping = createFieldIndexMapping(fieldNames, outputFieldNames);

		this.batchFlushEnable = userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_ENABLE, "false").equals("true");
		this.batchFlushMaxMutations = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS, "128"));
		this.batchFlushMaxSizeInBits = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB, "2")) * 1024 * 1024 * 8;
		this.batchFlushIntervalMillis = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, "1000"));
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		this.lock = new Object();
		this.connection = tableBuilder.buildConnection();
		this.table = tableBuilder.buildTable(connection);

		if (batchFlushEnable && batchFlushIntervalMillis > 0) {
			this.executor = new ScheduledThreadPoolExecutor(1);
			((HTable) table).setAutoFlush(false, false);
			executor.scheduleAtFixedRate(() -> {
				if (this.table != null && this.table instanceof HTable) {
					synchronized (lock) {
						try {
							flushToHBase();
						} catch (Exception e){
							log.warn("Scheduled flush operation to HBase cannot be finished.", e);
						}
					}
				}
			}, batchFlushIntervalMillis, batchFlushIntervalMillis, TimeUnit.MILLISECONDS);
		}
		isRunning = true;
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		Mutation mutation = extract(value);
		long mutationSize = mutation.heapSize();
		if (batchFlushEnable) {
			if (estimateSize != 0 && (estimateSize + mutationSize > batchFlushMaxSizeInBits || mutaionBuffer.size() + 1 > batchFlushMaxMutations)) {
				synchronized (lock){
					if (estimateSize != 0 && (estimateSize + mutationSize > batchFlushMaxSizeInBits
						|| mutaionBuffer.size() + 1 > batchFlushMaxMutations)) {
						long start = System.currentTimeMillis();
						Exception testException = null;
						try {
							flushToHBase();
						} catch (Exception e) {
							testException = e;
						}
						long end = System.currentTimeMillis();
						log.debug("Flush tasks " + (end - start) + " milliseconds with exception: " + testException);
					}
				}
			}
			synchronized (lock) {
				mutaionBuffer.add(mutation);
				estimateSize += mutation.heapSize();
			}
		} else if (mutation instanceof Put){
			table.put((Put) mutation);
		} else if (mutation instanceof Delete) {
			table.delete((Delete) mutation);
		} else if (mutation instanceof Append) {
			table.append((Append) mutation);
		} else if (mutation instanceof Increment) {
			table.increment((Increment) mutation);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws ExecutionException, InterruptedException {
		if (batchFlushEnable && this.table != null && this.table instanceof HTable) {
			synchronized (lock) {
				flushToHBase();
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// Do nothing here
	}

	protected Put generatePutMutation(IN value) {
		byte[] rowKey = HBaseUtils.serialize(fieldTypes[rowKeyIndex], produceElementWithIndex(value, rowKeyIndex));
		Put put = new Put(rowKey);
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != rowKeyIndex) {
				Object fieldValue = produceElementWithIndex(value, i);
				if (fieldValue != null) {
					put.addColumn(columnFamilies[i].getBytes(), qualifiers[i].getBytes(), HBaseUtils.serialize(fieldTypes[i], fieldValue));
				}
			}
		}
		return put;
	}

	protected Delete generateDeleteMutation(IN value) {
		byte[] rowKey = HBaseUtils.serialize(fieldTypes[rowKeyIndex], produceElementWithIndex(value, rowKeyIndex));
		Delete delete = new Delete(rowKey);
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != rowKeyIndex) {
				delete.addColumn(columnFamilies[i].getBytes(), qualifiers[i].getBytes());
			}
		}
		return delete;
	}

	protected abstract Mutation extract(IN value);

	protected abstract Object produceElementWithIndex(IN value, int index);

	private void flushToHBase() {
		long start = System.currentTimeMillis();
		try {
			if (isRunning && mutaionBuffer.size() > 0) {
				if (table == null) {
					log.error("HBase table cannot be null during flush.");
				} else {
					log.debug("mutation size is " + mutaionBuffer.size());
					table.batch(mutaionBuffer, new Object[mutaionBuffer.size()]);
					mutaionBuffer.clear();
					estimateSize = 0;
				}
			}
		} catch (Exception e) {
			log.warn("Fail to flush data into HBase due to: ", e);
			throw new RuntimeException(e);
		}

		log.debug("Flush mutations to HBase takes {} ms. ", System.currentTimeMillis() - start);
	}

	@Override
	public void close() {
		isRunning = false;

		if (executor != null) {
			executor.shutdown();
		}

		if (this.table != null) {
			try {
				this.table.close();
			} catch (IOException ioe) {
				log.warn("Exception occurs while closing HBase table.", ioe);
			}
			this.table = null;
		}

		if (this.connection != null) {
			try {
				this.connection.close();
			} catch (IOException ioe) {
				log.warn("Exception occurs while closing HBase connection.", ioe);
			}
			this.connection = null;
		}
	}

	private int[] createFieldIndexMapping(String[] fieldNames, String[] outputFieldNames) {
		int[] fieldElementIndexMapping = new int[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			fieldElementIndexMapping[i] = -1;
			for (int j = 0; j < outputFieldNames.length; j++) {
				if (fieldNames[i].equals(outputFieldNames[j])) {
					fieldElementIndexMapping[i] = j;
					break;
				}
			}
			if (fieldElementIndexMapping[i] == -1) {
				throw new RuntimeException("The field " + outputFieldNames[i] + " is not found in the result stream.");
			}
		}
		return fieldElementIndexMapping;
	}
}
