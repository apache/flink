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

package org.apache.flink.table.planner.runtime.utils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;

/**
 * POJOs for Table API testing.
 */
public class JavaPojos {

	/**
	 * Pojo1 for test.
	 */
	public static class Pojo1 implements Serializable {

		public Timestamp ts;
		public String msg;

		@Override
		public String toString() {
			return "Pojo1{" +
					"ts=" + ts +
					", msg='" + msg + '\'' +
					'}';
		}
	}

	/**
	 * Nested POJO.
	 */
	public static class Order {
		public Long user;
		public ProductItem product;
		public int amount;

		public Order() {
		}

		public Order(Long user, ProductItem product, int amount) {
			this.user = user;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + user +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}
	}

	/**
	 * Simple POJO.
	 */
	public static class ProductItem {
		public String name;
		public Long id;

		public ProductItem() {
		}

		public ProductItem(String name, Long id) {
			this.name = name;
			this.id = id;
		}

		@Override
		public String toString() {
			return "Product{" +
				"name='" + name + '\'' +
				", id=" + id +
				'}';
		}
	}

	/**
	 * POJO with a RAW type.
	 */
	public static class Device {
		public Long deviceId;
		public String deviceName;
		// raw type
		public TreeMap<String, Long> metrics;

		public Device() {
		}

		public Device(Long deviceId, String deviceName, Map<String, Long> metrics) {
			this.deviceId = deviceId;
			this.deviceName = deviceName;
			this.metrics = new TreeMap<>(metrics);
		}

		@Override
		public String toString() {
			return "Device{" +
				"deviceId=" + deviceId +
				", deviceName='" + deviceName + '\'' +
				", metrics=" + metrics +
				'}';
		}
	}
}
