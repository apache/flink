package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class BlinkStreamSQL {

	public static void main(String[] args)throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		DataStream<Order> orderA = env.fromCollection(Arrays.asList(
			new Order(1L, "beer", 3),
			new Order(1L, "diaper", 4),
			new Order(3L, "rubber", 2)));

		DataStream<Order> orderB = env.fromCollection(Arrays.asList(
			new Order(2L, "pen", 3),
			new Order(2L, "rubber", 3),
			new Order(4L, "beer", 1)));

		// convert DataStream to Table
		Table tableA = tEnv.fromDataStream(orderA, "productId, product, amount");
		// register DataStream as Table
		tEnv.registerDataStream("OrderB", orderB, "productId, product, amount");

		// union the two tables
		Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
			"SELECT * FROM OrderB WHERE amount < 2");

		tEnv.toAppendStream(result, Order.class).print();

		tEnv.execute("");
	}

	/**
	 * Simple POJO.
	 */
	public static class Order {
		public Long productId;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(Long productId, String product, int amount) {
			this.productId = productId;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"productId=" + productId +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}
	}
}
