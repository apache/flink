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

	public static final String HIVE_VERSION_V1_0_0 = "1.0.0";
	public static final String HIVE_VERSION_V1_0_1 = "1.0.1";
	public static final String HIVE_VERSION_V1_1_0 = "1.1.0";
	public static final String HIVE_VERSION_V1_1_1 = "1.1.1";
	public static final String HIVE_VERSION_V1_2_0 = "1.2.0";
	public static final String HIVE_VERSION_V1_2_1 = "1.2.1";
	public static final String HIVE_VERSION_V1_2_2 = "1.2.2";
	public static final String HIVE_VERSION_V2_0_0 = "2.0.0";
	public static final String HIVE_VERSION_V2_0_1 = "2.0.1";
	public static final String HIVE_VERSION_V2_1_0 = "2.1.0";
	public static final String HIVE_VERSION_V2_1_1 = "2.1.1";
	public static final String HIVE_VERSION_V2_2_0 = "2.2.0";
	public static final String HIVE_VERSION_V2_3_0 = "2.3.0";
	public static final String HIVE_VERSION_V2_3_1 = "2.3.1";
	public static final String HIVE_VERSION_V2_3_2 = "2.3.2";
	public static final String HIVE_VERSION_V2_3_3 = "2.3.3";
	public static final String HIVE_VERSION_V2_3_4 = "2.3.4";
	public static final String HIVE_VERSION_V2_3_5 = "2.3.5";
	public static final String HIVE_VERSION_V2_3_6 = "2.3.6";
	public static final String HIVE_VERSION_V3_1_0 = "3.1.0";
	public static final String HIVE_VERSION_V3_1_1 = "3.1.1";
	public static final String HIVE_VERSION_V3_1_2 = "3.1.2";

	private static final Map<String, HiveShim> hiveShims = new ConcurrentHashMap<>(2);

	private static final Logger LOG = LoggerFactory.getLogger(HiveShimLoader.class);

	private HiveShimLoader() {
	}

	public static HiveShim loadHiveShim(String version) {
		return hiveShims.computeIfAbsent(version, (v) -> {
			if (v.startsWith(HIVE_VERSION_V1_0_0)) {
				return new HiveShimV100();
			}
			if (v.startsWith(HIVE_VERSION_V1_0_1)) {
				return new HiveShimV101();
			}
			if (v.startsWith(HIVE_VERSION_V1_1_0)) {
				return new HiveShimV110();
			}
			if (v.startsWith(HIVE_VERSION_V1_1_1)) {
				return new HiveShimV111();
			}
			if (v.startsWith(HIVE_VERSION_V1_2_0)) {
				return new HiveShimV120();
			}
			if (v.startsWith(HIVE_VERSION_V1_2_1)) {
				return new HiveShimV121();
			}
			if (v.startsWith(HIVE_VERSION_V1_2_2)) {
				return new HiveShimV122();
			}
			if (v.startsWith(HIVE_VERSION_V2_0_0)) {
				return new HiveShimV200();
			}
			if (v.startsWith(HIVE_VERSION_V2_0_1)) {
				return new HiveShimV201();
			}
			if (v.startsWith(HIVE_VERSION_V2_1_0)) {
				return new HiveShimV210();
			}
			if (v.startsWith(HIVE_VERSION_V2_1_1)) {
				return new HiveShimV211();
			}
			if (v.startsWith(HIVE_VERSION_V2_2_0)) {
				return new HiveShimV220();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_0)) {
				return new HiveShimV230();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_1)) {
				return new HiveShimV231();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_2)) {
				return new HiveShimV232();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_3)) {
				return new HiveShimV233();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_4)) {
				return new HiveShimV234();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_5)) {
				return new HiveShimV235();
			}
			if (v.startsWith(HIVE_VERSION_V2_3_6)) {
				return new HiveShimV236();
			}
			if (v.startsWith(HIVE_VERSION_V3_1_0)) {
				return new HiveShimV310();
			}
			if (v.startsWith(HIVE_VERSION_V3_1_1)) {
				return new HiveShimV311();
			}
			if (v.startsWith(HIVE_VERSION_V3_1_2)) {
				return new HiveShimV312();
			}
			throw new CatalogException("Unsupported Hive version " + v);
		});
	}

	public static String getHiveVersion() {
		return HiveVersionInfo.getVersion();
	}
}
