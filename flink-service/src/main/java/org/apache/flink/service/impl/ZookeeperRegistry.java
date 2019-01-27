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

package org.apache.flink.service.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.ServiceInstance;
import org.apache.flink.service.ServiceRegistry;
import org.apache.flink.util.InstantiationUtil;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * ZookeeperRegistry is a Zookeeper based implementation for {@link ServiceRegistry}.
 */
public class ZookeeperRegistry implements ServiceRegistry, Watcher {

	public static final String ZOOKEEPER_REGISTRY_QUORUM = "flink.service.registry.zookeeper.quorum";

	public static final String ZOOKEEPER_REGISTRY_PATH = "flink.service.registry.zookeeper.rootpath";

	private static final String ZOOKEEPER_REGISTRY_DEFAULT_QUORUM_VALUE = "127.0.0.1:2181";

	private static final String ZOOKEEPER_REGISTRY_DEFAULT_PATH_VALUE = "/flink_zookeeper_registry";

	private static final Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

	private volatile ZooKeeper zooKeeper;

	private String zkQuorum;

	private String zkRootPath;

	private CountDownLatch countDownLatch = new CountDownLatch(1);

	@Override
	public void addInstance(String serviceName, String instanceId, String serviceIp, int port, byte[] customData) {
		ServiceInstance serviceInfo = new ServiceInstance(serviceName, instanceId);
		serviceInfo.setServiceIp(serviceIp);
		serviceInfo.setServicePort(port);
		if (customData != null && customData.length > 0) {
			serviceInfo.setCustomData(customData);
		}

		List<String> paths = new ArrayList<>();
		for (String subDir : zkRootPath.split("/")) {
			if (!subDir.isEmpty()) {
				paths.add(subDir);
			}
		}
		paths.add(serviceInfo.getServiceName());
		paths.add(serviceInfo.getInstanceId());

		String path = paths.stream().collect(Collectors.joining("/", "/", ""));
		try {
			if (zooKeeper.exists(path, false) != null) {
				zooKeeper.delete(path, -1);
			}
			createPath(paths);
			zooKeeper.setData(path, serviceInfoToBytes(serviceInfo), 0);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private void createPath(List<String> folders) {

		String path = folders.stream().collect(Collectors.joining("/", "/", ""));

		try {
			if (zooKeeper.exists(path, false) == null) {
				createPath(folders.subList(0, folders.size() - 1));
				zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void removeInstance(String serviceName, String instanceId) {
		String path = zkRootPath + "/" + serviceName + "/" + instanceId;
		try {
			if (zooKeeper.exists(path, false) != null) {
				zooKeeper.delete(path, -1);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<ServiceInstance> getAllInstances(String serviceName) {

		List<ServiceInstance> serviceInfoList = new ArrayList<>();

		String path = zkRootPath + "/" + serviceName;
		try {
			List<String> children = zooKeeper.getChildren(path, false);
			if (children != null) {
				for (String childName : children) {
					String childPath = path + "/" + childName;
					byte[] buffer = zooKeeper.getData(childPath, false, null);
					serviceInfoList.add(createServiceInfo(buffer));
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

		return serviceInfoList;
	}

	@Override
	public void open(Configuration config) {
		this.zkQuorum = config.getString(ZOOKEEPER_REGISTRY_QUORUM, ZOOKEEPER_REGISTRY_DEFAULT_QUORUM_VALUE);
		this.zkRootPath = config.getString(ZOOKEEPER_REGISTRY_PATH, ZOOKEEPER_REGISTRY_DEFAULT_PATH_VALUE);
		try {
			zooKeeper = new ZooKeeper(zkQuorum, 3000, this);
			countDownLatch.await();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (zooKeeper != null) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	private static byte[] serviceInfoToBytes(ServiceInstance serviceInfo) throws IOException {
		return InstantiationUtil.serializeObject(serviceInfo);
	}

	private static ServiceInstance createServiceInfo(byte[] buffer) throws IOException, ClassNotFoundException {
		ServiceInstance serviceInfo = InstantiationUtil.deserializeObject(buffer, ZookeeperRegistry.class.getClassLoader());
		return serviceInfo;
	}

	@Override
	public void process(WatchedEvent event) {
		logger.info("ZookeeperRegistry receive event: " + event);
		if (event.getState() == Event.KeeperState.SyncConnected) {
			countDownLatch.countDown();
		}
	}
}
