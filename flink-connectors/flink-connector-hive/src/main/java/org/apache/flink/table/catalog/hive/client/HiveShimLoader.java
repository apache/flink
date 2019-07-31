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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A loader to load HiveShim.
 */
public class HiveShimLoader {

	public static final String HIVE_V1_VERSION_NAME = "1.2.1";
	public static final String HIVE_V2_VERSION_NAME = "2.3.4";

	private static final Map<String, HiveShim> hiveShims = new ConcurrentHashMap<>(2);

	private static final Logger LOG = LoggerFactory.getLogger(HiveShimLoader.class);

	private HiveShimLoader() {
	}

	public static HiveShim loadHiveShim(String version) {
		return hiveShims.computeIfAbsent(version, (v) -> {
			if (v.startsWith(HIVE_V1_VERSION_NAME)) {
				return new HiveShimV1();
			}
			if (v.startsWith(HIVE_V2_VERSION_NAME)) {
				return new HiveShimV2();
			}
			throw new CatalogException("Unsupported Hive version " + v);
		});
	}

	public static String getHiveVersion() {
		return HiveVersionInfo.getVersion();
	}
}
