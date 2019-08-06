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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.table.descriptors.Bucket;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Parquet;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import com.alibaba.fastjson.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>This example shows how to:
 * - Convert DataStreams to Tables - Register a Table under a name - Run a StreamSQL query on the
 * registered Table
 */
public class FileSystemSinkTest {

	StreamExecutionEnvironment env;
	StreamTableEnvironment tEnv;

	@BeforeClass
	public void init() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		env.enableCheckpointing(5000);
		tEnv = StreamTableEnvironment.create(env);

	}

	@Test
	public void testKafka2Hdfs() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("test3",
			new SimpleStringSchema(),
			properties);
		myConsumer.setStartFromLatest();

		StreamingFileSink<String> sink = StreamingFileSink
			.forRowFormat(new Path("hdfs://localhost/tmp/flink-data/json"),
				new SimpleStringEncoder<String>("UTF-8"))
			.build();

		DataStream ds = env.addSource(myConsumer);
//		ds.print();
		ds.addSink(sink);
		env.execute();
	}

	@Test
	public void testKafka2File() throws Exception {
		StreamingFileSink<Row> sink = StreamingFileSink
			.forRowFormat(new Path("hdfs://localhost/tmp/flink-data/json"),
				new SimpleStringEncoder<Row>("UTF-8"))
			.build();

		BucketingSink sink1 = new BucketingSink<Row>("hdfs://localhost/tmp/flink-data/json");

		registerOrder();
		Table result = tEnv.sqlQuery("SELECT * FROM mysource");
		tEnv.toAppendStream(result, Row.class).addSink(sink1);
		env.execute();
	}

	@Test
	public void testKafka2Parquet() throws Exception {
		StreamingFileSink<Address> sink = StreamingFileSink.forBulkFormat(
			new Path("file:///tmp/flink-data/parquet"),
			ParquetAvroWriters.forSpecificRecord(Address.class))
			.build();

		registerAddresFromKafka();

		Table table = tEnv.sqlQuery("select * from mysource");
		tEnv.toAppendStream(table, Row.class).print();
		env.execute();
	}

	@Test
	public void testFileJson() throws Exception {

		registerOrder();

		tEnv.connect(new Bucket().basePath("file:///tmp/flink-data/json").rawFormat()
			.dateFormat("yyyy-MM-dd-HHmm"))
			.withFormat(new Json().deriveSchema())
			.withSchema(new Schema()
				.field("a", Types.STRING())
				.field("b", Types.STRING())
				.field("c", Types.LONG()))
			.inAppendMode()
			.registerTableSink("mysink");

		tEnv.sqlUpdate("insert into mysink SELECT * FROM mysource");

//		Table table = tEnv.sqlQuery("select * from mysource");
//		tEnv.toAppendStream(table, Row.class).print();

		env.execute();
	}

	@Test
	public void testFileCsv() throws Exception {

		registerOrder();

		char c = 0x01;
		tEnv.connect(new Bucket().basePath("file:///tmp/flink-data/csv").rawFormat()
			.dateFormat("yyyy-MM-dd-HHmm"))
			.withFormat(new Csv().deriveSchema().fieldDelimiter(c))
			.withSchema(new Schema()
				.field("a", Types.STRING())
				.field("q", Types.STRING())
				.field("w", Types.LONG()))
			.inAppendMode()
			.registerTableSink("mysink");
		tEnv.sqlUpdate("insert into mysink SELECT * FROM mysource");

		env.execute();
	}

	@Test
	public void testFileAvro() throws Exception {
//		TableSchema ts = TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(Address.class));
		registerAddresFromKafka();

		tEnv.connect(new Bucket().basePath("file:///tmp/flink-data/avro").bultFormat()
			.dateFormat("yyyy-MM-dd-HHmm"))
//			.withFormat(new Avro().recordClass(Address.class))
			.withFormat(new Avro().avroSchema(
				"{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"org.apache.flink.formats.avro.generated\",\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}"))
			.inAppendMode()
			.registerTableSink("mysink");

//		tEnv.connect(
//			new Kafka()
//				.version("0.10")
//				.topic("kafkaSinkAvro")
//				.property("bootstrap.servers", "localhost:9092"))
//			.withFormat(new Avro().recordClass(Address.class))
//			.withSchema(new Schema().schema(ts))
//			.inAppendMode()
//			.registerTableSink("mysink");

//		Table result = tEnv.sqlQuery("SELECT * FROM mysource");
//		tEnv.toAppendStream(result, Row.class).print();

		tEnv.sqlUpdate("insert into mysink SELECT * FROM mysource");
//		tEnv.sqlQuery("SELECT city FROM mysource");

		env.execute();
	}

	@Test
	public void testFileParquet() throws Exception {
		registerAddresFromKafka();

		String schemaStr = "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"org.apache.flink.formats.avro.generated\",\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}";
		org.apache.avro.Schema avroschema = new org.apache.avro.Schema.Parser().parse(schemaStr);

		tEnv.connect(new Bucket().basePath("file:///tmp/flink-data/parquet").bultFormat()
			.dateFormat("yyyy-MM-dd-HHmm"))
			.withFormat(new Parquet().reflectClass(Address.class))
			.inAppendMode()
			.registerTableSink("mysink");
		tEnv.sqlUpdate("insert into mysink SELECT * FROM mysource");

		env.execute();
	}

	@Test
	public void testWrite() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(5000);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010("kafkaSourceJson",
			new SimpleStringSchema(),
			properties);
		myConsumer.setStartFromLatest();

		StreamingFileSink<Address> sink = StreamingFileSink.forBulkFormat(
			new Path("file:///tmp/flink-data/a"),
			new AvroWriterFactory(Address.getClassSchema().toString()))
			.build();

//		StreamingFileSink<Address> sink = StreamingFileSink.forBulkFormat(
//			new Path("file:///tmp/flink-data/a"),
//			ParquetAvroWriters.forSpecificRecord(Address.class))
//			.build();

		DataStream ds = env.addSource(myConsumer).map(new MapFunction<String, Address>() {
			@Override
			public Address map(String value) throws Exception {
				JSONObject json = JSONObject.parseObject(value);
				Address address = new Address();
				address.setNum(json.getIntValue("num"));
				address.setStreet(json.getString("street"));
				address.setCity(json.getString("city"));
				address.setState(json.getString("state"));
				address.setZip(json.getString("zip"));

				return address;
			}
		});
		ds.addSink(sink);

		env.execute();

	}

	private void registerOrder() {
//		DataStream<Order> orderB = env.fromCollection(Arrays.asList(
//			new Order(2L, "pen", 3),
//			new Order(2L, "rubber", 3),
//			new Order(4L, "beer", 1)));
//
//		tEnv.registerDataStream("OrderB", orderB, "user, product, amount");

		DataStream<Tuple3<String, String, Long>> dataStream = env.addSource(new MySource()).returns(
			org.apache.flink.api.common.typeinfo.Types
				.TUPLE(org.apache.flink.api.common.typeinfo.Types.STRING,
					org.apache.flink.api.common.typeinfo.Types.STRING,
					org.apache.flink.api.common.typeinfo.Types.LONG));
		tEnv.registerDataStream("mysource", dataStream, "appName,clientIp,uploadTime");
//		tEnv.connect(new Kafka()
//			.version("0.10")
//			.topic("test3")
//			.property("bootstrap.servers", "localhost:9092"))
//			.withFormat(new Json().deriveSchema())
//			.withSchema(new Schema()
//				.field("appName", Types.STRING())
//				.field("clientIp", Types.STRING())
//				.field("uploadTime", Types.LONG())
//			)
//			.inAppendMode()
//			.registerTableSource("mysource");
	}

	private void registerAddresFromKafka() {
//		tEnv.connect(new Kafka()
//			.version("0.10")
//			.topic("kafkaSourceJson").startFromLatest()
//			.property("bootstrap.servers", "localhost:9092"))
//			.withFormat(new Json().deriveSchema())
//			.withSchema(new Schema()
//				.field("num", Types.INT())
//				.field("city", Types.STRING())
//				.field("state", Types.STRING())
//				.field("zip", Types.STRING())
//				.field("street", Types.STRING())
//			)
//			.inAppendMode()
//			.registerTableSource("mysource");

		DataStream<Tuple5<Integer, String, String, String, String>> dataStream = env
			.addSource(new MySourceAvro()).returns(org.apache.flink.api.common.typeinfo.Types
				.TUPLE(org.apache.flink.api.common.typeinfo.Types.INT,
					org.apache.flink.api.common.typeinfo.Types.STRING,
					org.apache.flink.api.common.typeinfo.Types.STRING,
					org.apache.flink.api.common.typeinfo.Types.STRING,
					org.apache.flink.api.common.typeinfo.Types.STRING));
		tEnv.registerDataStream("mysource", dataStream, "num, street, city,state,zip");

	}

	/**
	 *
	 */
	public static class MySource implements SourceFunction<Tuple3<String, String, Long>> {

		private volatile boolean isRunning = true;
		long n = 1;

		@Override
		public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
			while (isRunning) {
				Thread.sleep((int) (Math.random() * 100));
				Tuple3<String, String, Long> tuple3 = new Tuple3("appName" + n++, "clientIp" + n++,
					n++);
				ctx.collect(tuple3);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	/**
	 *
	 */
	public static class MySourceAvro implements
		SourceFunction<Tuple5<Integer, String, String, String, String>> {

		private volatile boolean isRunning = true;
		int n = 1;

		@Override
		public void run(SourceContext<Tuple5<Integer, String, String, String, String>> ctx)
			throws Exception {
			while (isRunning) {
				Thread.sleep((int) (Math.random() * 100));
				Tuple5<Integer, String, String, String, String> tuple5 = new Tuple5(n,
					"street" + n++, "city" + n++, "state" + n++, "zip" + n++);
				ctx.collect(tuple5);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	/**
	 * Simple POJO.
	 */
	public static class Order {

		public Long user;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(Long user, String product, int amount) {
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
}
