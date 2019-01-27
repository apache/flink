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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.ServiceRegistry;
import org.apache.flink.service.UserDefinedService;
import org.apache.flink.table.temptable.rpc.TableServiceServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;

/**
 * The built-in implementation of TableService.
 */
public class FlinkTableService extends UserDefinedService {

	private static final Logger logger = LoggerFactory.getLogger(FlinkTableService.class);

	private TableServiceServer server;

	private String serviceIP;

	private int servicePort = -1;

	private TableServiceImpl manager;

	private ServiceRegistry registry;

	private String tableServiceId;

	@Override
	public void open(Configuration config) throws Exception {
		tableServiceId = config.getString(TableServiceOptions.TABLE_SERVICE_ID);
		logger.info("start table service with id:" + tableServiceId);
		registry = getServiceContext().getRegistry();
		registry.open(config);
		manager = new TableServiceImpl(getServiceContext());
		manager.open(config);
		setUpServer();
		addInstance();
	}

	@Override
	public void close() throws Exception {
		if (server != null) {
			server.stop();
		}
		if (registry != null) {
			removeInstance();
			registry.close();
		}

		manager.close();
	}

	private void setUpServer() {
		logger.info("begin set up table service.");

		try {
			serviceIP = Inet4Address.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

		server = new TableServiceServer();
		server.setTableService(manager);

		try {
			servicePort = server.bind();
		} catch (Exception e) {
			logger.error("start server fail.", e);
			throw new RuntimeException(e);
		}

		logger.info("end set up table service.");
	}

	private void addInstance() {
		int subIndex = getServiceContext().getIndexOfCurrentInstance();
		int parallelism = getServiceContext().getNumberOfInstances();
		registry.addInstance(tableServiceId, subIndex + "_" + parallelism, serviceIP, servicePort, null);
	}

	private void removeInstance() {
		int subIndex = getServiceContext().getIndexOfCurrentInstance();
		int parallelism = getServiceContext().getNumberOfInstances();
		registry.removeInstance(tableServiceId,  subIndex + "_" + parallelism);
	}

	@Override
	public void start() {
		logger.info("TableService begin serving");
		try {
			server.start();
		} catch (Exception e) {
			logger.error("error occurs while serving: " + e);
		}
		logger.error("TableService end serving");
		throw new RuntimeException("TableService stops.");
	}
}
