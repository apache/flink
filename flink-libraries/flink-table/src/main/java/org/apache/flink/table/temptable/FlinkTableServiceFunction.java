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

package org.apache.flink.table.temptable;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.service.ServiceContext;
import org.apache.flink.service.ServiceDescriptor;
import org.apache.flink.service.ServiceRegistry;
import org.apache.flink.service.ServiceRegistryFactory;
import org.apache.flink.service.UserDefinedService;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.dataformat.BaseRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The Wrapper for FlinkTableService.
 */
public class FlinkTableServiceFunction extends RichParallelSourceFunction<BaseRow> implements StoppableFunction {

	private static final Logger logger = LoggerFactory.getLogger(FlinkTableServiceFunction.class);

	private ServiceDescriptor serviceDescriptor;

	private String tableServiceId;

	private UserDefinedService service;

	private ExecutorService executorService;

	private volatile boolean closed;

	public FlinkTableServiceFunction(ServiceDescriptor serviceDescriptor) {
		this.serviceDescriptor = serviceDescriptor;
	}

	@Override
	public void run(SourceFunction.SourceContext<BaseRow> ctx) throws Exception {
		executorService.submit(() -> service.start());
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1L, TimeUnit.SECONDS);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		logger.info("Table Service is done.");
	}

	@Override
	public void cancel() {
		try {
			close();
		} catch (Exception e){
			logger.error(e.getMessage(), e);
		}
		logger.info("TableService " + getRuntimeContext().getTaskNameWithSubtasks() + " canceled");
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		tableServiceId = serviceDescriptor.getConfiguration().getString(TableServiceOptions.TABLE_SERVICE_ID);
		service = (UserDefinedService) Class.forName(serviceDescriptor.getServiceClassName()).newInstance();
		service.setServiceContext(new ServiceContext() {
			@Override
			public ServiceRegistry getRegistry() {
				return ServiceRegistryFactory.getRegistry();
			}

			@Override
			public MetricGroup getMetricGroup() {
				return getRuntimeContext().getMetricGroup();
			}

			@Override
			public int getNumberOfInstances() {
				return getRuntimeContext().getNumberOfParallelSubtasks();
			}

			@Override
			public int getIndexOfCurrentInstance() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}
		});
		service.open(serviceDescriptor.getConfiguration());
		executorService = Executors.newFixedThreadPool(1);
		logger.info("TableService " + getRuntimeContext().getTaskNameWithSubtasks() + " opened");
	}

	@Override
	public void close() throws Exception {
		if (!closed) {
			service.close();
			executorService.shutdown();
			super.close();
			logger.info("TableService " + getRuntimeContext().getTaskNameWithSubtasks() + " closed");
			closed = true;
		}
	}

	@Override
	public void stop() {
		try {
			close();
		} catch (Exception e){
			logger.error(e.getMessage(), e);
		}
		logger.info("TableService " + getRuntimeContext().getTaskNameWithSubtasks() + " stopped");
	}
}
