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

package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.TimeZone;


/**
 * Simple example that shows the use of SQL Join on Stream Tables.
 */
public class StreamJoinSQLExample {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		DataStream<Order> order = env.fromElements(
			new Order(Timestamp.valueOf("2018-10-15 09:01:20"), 2, 1, 7),
			new Order(Timestamp.valueOf("2018-10-15 09:05:02"), 3, 2, 9),
			new Order(Timestamp.valueOf("2018-10-15 09:05:02"), 1, 3, 9),
			new Order(Timestamp.valueOf("2018-10-15 10:07:22"), 1, 4, 9),
			new Order(Timestamp.valueOf("2018-10-15 10:55:01"), 5, 5, 8));
		DataStream<Shipment> shipment = env.fromElements(
			new Shipment(Timestamp.valueOf("2018-10-15 09:11:00"), 3),
			new Shipment(Timestamp.valueOf("2018-10-15 10:01:21"), 1),
			new Shipment(Timestamp.valueOf("2018-10-15 11:31:10"), 5));

		// register the DataStreams under the name "t_order" and "t_shipment"
		tEnv.registerDataStream("t_order", order, "createTime, unit, orderId, productId");
		tEnv.registerDataStream("t_shipment", shipment, "createTime, orderId");

		// run a SQL to get orders whose ship date are within one hour of the order date
		Table table = tEnv.sqlQuery(
			"SELECT o.createTime, o.productId, o.orderId, s.createTime AS shipTime" +
				" FROM t_order AS o" +
				" JOIN t_shipment AS s" +
				"  ON o.orderId = s.orderId" +
				"  AND s.createTime BETWEEN o.createTime AND o.createTime + INTERVAL '1' HOUR");

		DataStream<Row> resultDataStream = tEnv.toAppendStream(table, Row.class);
		resultDataStream.print();
		env.execute();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Order(createTime, unit, orderId, productId).
	 */
	public static class Order {
		public Timestamp createTime;
		public int unit;
		public long orderId;
		public long productId;

		public Order() {
		}

		public Order(Timestamp createTime, int unit, long orderId, long productId) {
			this.createTime = createTime;
			this.unit = unit;
			this.orderId = orderId;
			this.productId = productId;
		}

		@Override
		public String toString() {
			return "Order " + createTime + " " + unit + " " + orderId + " " + productId;
		}
	}

	/**
	 * Shipment(createTime, orderId).
	 */
	public static class Shipment {
		public Timestamp createTime;
		public long orderId;

		// public constructor to make it a Flink POJO

		public Shipment() {
		}

		public Shipment(Timestamp createTime, long orderId) {
			this.createTime = createTime;
			this.orderId = orderId;
		}

		@Override
		public String toString() {
			return "Shipment " + createTime + " " + orderId;
		}
	}
}
