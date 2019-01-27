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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * FileSystemRegistry is local based implementation of {@link ServiceRegistry}.
 * only Used for Local Environment.
 */
public class FileSystemRegistry implements ServiceRegistry {

	private static final Logger logger = LoggerFactory.getLogger(FileSystemRegistry.class);

	public static final String FILESYSTEM_REGISTRY_PATH = "flink.service.registry.filesystem.rootpath";

	private static final String FILESYSTEM_REGISTRY_DEFAULT_PATH_VALUE = "file_system_registry";

	private String rootPath;

	@Override
	public void addInstance(String serviceName, String instanceId, String serviceIp, int port, byte[] customData) {
		ServiceInstance serviceInfo = new ServiceInstance(serviceName, instanceId);
		serviceInfo.setServiceIp(serviceIp);
		serviceInfo.setServicePort(port);
		if (customData != null && customData.length > 0) {
			serviceInfo.setCustomData(customData);
		}

		String servicePath = rootPath + File.separator + serviceName;

		File serviceFile = new File(servicePath);
		if (!serviceFile.exists()) {
			serviceFile.mkdirs();
		}

		File file = new File(servicePath + File.separator + instanceId);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}

		byte[] bytes = null;
		try {
			bytes = InstantiationUtil.serializeObject(serviceInfo);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}

		FileOutputStream fw = null;
		try {
			fw = new FileOutputStream(file);
			fw.write(bytes);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}

	}

	@Override
	public void removeInstance(String serviceName, String instanceId) {
		String path = rootPath + File.separator + serviceName + File.separator + instanceId;
		File file = new File(path);
		if (file.exists()) {
			file.delete();
		}
	}

	@Override
	public List<ServiceInstance> getAllInstances(String serviceName) {

		String path = rootPath + File.separator + serviceName;
		File file = new File(path);

		if (!file.exists()) {
			return Collections.emptyList();
		} else {
			List<ServiceInstance> resList = new ArrayList<>();
			for (File subFile : file.listFiles()) {
				int fileLength = (int) subFile.length();
				byte []content = new byte[fileLength];
				FileInputStream fis = null;
				try {
					fis = new FileInputStream(subFile);
					fis.read(content, 0, fileLength);
					resList.add(InstantiationUtil.deserializeObject(content, ServiceInstance.class.getClassLoader()));
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw new RuntimeException(e);
				} finally {
					if (fis != null) {
						try {
							fis.close();
						} catch (IOException e) {
							logger.error(e.getMessage(), e);
						}
					}
				}

			}
			return resList;
		}
	}

	@Override
	public void open(Configuration config) {
		this.rootPath = config.getString(
			FILESYSTEM_REGISTRY_PATH,
			System.getProperty("user.dir") + File.separator + FILESYSTEM_REGISTRY_DEFAULT_PATH_VALUE);
	}

	@Override
	public void close() {
		File rootDir = new File(rootPath);
		synchronized (FileSystemRegistry.class) {
			if (rootDir.exists()) {
				deleteAll(rootDir);
			}
		}
	}

	private void deleteAll(File rootDir) {
		if (rootDir.isFile()) {
			rootDir.delete();
		} else {
			for (File subDir : rootDir.listFiles()) {
				deleteAll(subDir);
			}
			rootDir.delete();
		}
	}
}

