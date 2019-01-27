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

package org.apache.flink.table.temptable.util;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.ServiceDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.temptable.FlinkTableServiceFactory;
import org.apache.flink.table.temptable.FlinkTableServiceFactoryDescriptor;
import org.apache.flink.table.temptable.FlinkTableServiceFunction;
import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.rpc.TableServiceClient;
import org.apache.flink.table.util.TableProperties;

import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CLASS_NAME;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_CPU_CORES;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_DIRECT_MEMORY_MB;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_HEAP_MEMORY_MB;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_NATIVE_MEMORY_MB;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_PARALLELISM;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_READY_RETRY_BACKOFF_MS;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_READY_RETRY_TIMES;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH;
import static org.apache.flink.table.temptable.TableServiceOptions.TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE;

/**
 * Helper class for TableService.
 */
public final class TableServiceUtil {

	private TableServiceUtil() {}

	public static void createTableServiceJob(StreamExecutionEnvironment env, ServiceDescriptor serviceDescriptor) {

		ResourceSpec resourceSpec = ResourceSpec.newBuilder()
			.setCpuCores(serviceDescriptor.getServiceCpuCores())
			.setHeapMemoryInMB(serviceDescriptor.getServiceHeapMemoryMb())
			.setDirectMemoryInMB(serviceDescriptor.getServiceDirectMemoryMb())
			.setNativeMemoryInMB(serviceDescriptor.getServiceNativeMemoryMb())
			.build();

		DataStream<BaseRow> ds = env.addSource(new FlinkTableServiceFunction(serviceDescriptor))
			.setParallelism(serviceDescriptor.getServiceParallelism())
			.setMaxParallelism(serviceDescriptor.getServiceParallelism());

		ds.addSink(new SinkFunction<BaseRow>() {
			@Override
			public void invoke(BaseRow value, Context context) {

			}
		}).setParallelism(serviceDescriptor.getServiceParallelism());
	}

	public static ServiceDescriptor createTableServiceDescriptor(Configuration config) {
		ServiceDescriptor tableServiceDescriptor = new ServiceDescriptor()
			.setServiceClassName(config.getString(TABLE_SERVICE_CLASS_NAME))
			.setServiceParallelism(config.getInteger(TABLE_SERVICE_PARALLELISM))
			.setServiceHeapMemoryMb(config.getInteger(TABLE_SERVICE_HEAP_MEMORY_MB))
			.setServiceDirectMemoryMb(config.getInteger(TABLE_SERVICE_DIRECT_MEMORY_MB))
			.setServiceNativeMemoryMb(config.getInteger(TABLE_SERVICE_NATIVE_MEMORY_MB))
			.setServiceCpuCores(config.getDouble(TABLE_SERVICE_CPU_CORES));

		tableServiceDescriptor.getConfiguration().addAll(config);

		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_READY_RETRY_TIMES, config.getInteger(TABLE_SERVICE_READY_RETRY_TIMES));
		tableServiceDescriptor.getConfiguration().setLong(TABLE_SERVICE_READY_RETRY_BACKOFF_MS, config.getLong(TABLE_SERVICE_READY_RETRY_BACKOFF_MS));
		if (config.getString(TABLE_SERVICE_STORAGE_ROOT_PATH) != null) {
			tableServiceDescriptor.getConfiguration().setString(TABLE_SERVICE_STORAGE_ROOT_PATH, config.getString(TABLE_SERVICE_STORAGE_ROOT_PATH));
		}
		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE, config.getInteger(TABLE_SERVICE_STORAGE_SEGMENT_MAX_SIZE));
		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE, config.getInteger(TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE));
		tableServiceDescriptor.getConfiguration().setInteger(TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE, config.getInteger(TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE));

		return tableServiceDescriptor;
	}

	public static FlinkTableServiceFactoryDescriptor getDefaultTableServiceFactoryDescriptor(){
		return new FlinkTableServiceFactoryDescriptor(
			new FlinkTableServiceFactory(), new TableProperties());
	}

	public static void checkTableServiceReady(TableServiceClient client, int maxRetryTimes, long backOffMs) {
		int retryTime = 0;
		while (retryTime++ < maxRetryTimes) {
			if (client.isReady()) {
				return;
			}
			try {
				Thread.sleep(backOffMs);
			} catch (InterruptedException e) {}
		}
		throw new TableServiceException(new RuntimeException("TableService is not ready"));
	}
}
