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

package org.apache.flink.formats.json.canal;

import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link CanalJsonSerializationSchema} and {@link CanalJsonDeserializationSchema}.
 */
public class CanalJsonSerDeSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final RowType SCHEMA = (RowType) ROW(
		FIELD("id", INT().notNull()),
		FIELD("name", STRING()),
		FIELD("description", STRING()),
		FIELD("weight", FLOAT())
	).getLogicalType();

	@Test
	public void testFilteringTables() throws Exception {
		List<String> lines = readLines("canal-data-filter-table.txt");
		CanalJsonDeserializationSchema deserializationSchema = CanalJsonDeserializationSchema
			.builder(SCHEMA, InternalTypeInfo.of(SCHEMA))
			.setDatabase("mydb")
			.setTable("product")
			.build();
		runTest(lines, deserializationSchema);
	}

	@Test
	public void testSerializationDeserialization() throws Exception {
		List<String> lines = readLines("canal-data.txt");
		CanalJsonDeserializationSchema deserializationSchema = CanalJsonDeserializationSchema
			.builder(SCHEMA, InternalTypeInfo.of(SCHEMA))
			.setIgnoreParseErrors(false)
			.setTimestampFormat(TimestampFormat.ISO_8601)
			.build();
		runTest(lines, deserializationSchema);
	}

	public void runTest(List<String> lines, CanalJsonDeserializationSchema deserializationSchema) throws Exception {
		SimpleCollector collector = new SimpleCollector();
		for (String line : lines) {
			deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
		}

		// Canal captures change data (`canal-data.txt`) on the `product` table:
		//
		// CREATE TABLE product (
		//  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
		//  name VARCHAR(255),
		//  description VARCHAR(512),
		//  weight FLOAT
		// );
		// ALTER TABLE product AUTO_INCREMENT = 101;
		//
		// INSERT INTO product
		// VALUES (default,"scooter","Small 2-wheel scooter",3.14),
		//        (default,"car battery","12V car battery",8.1),
		//        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
		//        (default,"hammer","12oz carpenter's hammer",0.75),
		//        (default,"hammer","14oz carpenter's hammer",0.875),
		//        (default,"hammer","16oz carpenter's hammer",1.0),
		//        (default,"rocks","box of assorted rocks",5.3),
		//        (default,"jacket","water resistent black wind breaker",0.1),
		//        (default,"spare tire","24 inch spare tire",22.2);
		// UPDATE product SET description='18oz carpenter hammer' WHERE id=106;
		// UPDATE product SET weight='5.1' WHERE id=107;
		// INSERT INTO product VALUES (default,"jacket","water resistent white wind breaker",0.2);
		// INSERT INTO product VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
		// UPDATE product SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
		// UPDATE product SET weight='5.17' WHERE id=111;
		// DELETE FROM product WHERE id=111;
		// UPDATE products SET weight='5.17' WHERE id=102 or id = 101;
		// DELETE FROM products WHERE id=102 or id = 103;
		List<String> expected = Arrays.asList(
			"+I(101,scooter,Small 2-wheel scooter,3.14)",
			"+I(102,car battery,12V car battery,8.1)",
			"+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8)",
			"+I(104,hammer,12oz carpenter's hammer,0.75)",
			"+I(105,hammer,14oz carpenter's hammer,0.875)",
			"+I(106,hammer,16oz carpenter's hammer,1.0)",
			"+I(107,rocks,box of assorted rocks,5.3)",
			"+I(108,jacket,water resistent black wind breaker,0.1)",
			"+I(109,spare tire,24 inch spare tire,22.2)",
			"-U(106,hammer,16oz carpenter's hammer,1.0)",
			"+U(106,hammer,18oz carpenter hammer,1.0)",
			"-U(107,rocks,box of assorted rocks,5.3)",
			"+U(107,rocks,box of assorted rocks,5.1)",
			"+I(110,jacket,water resistent white wind breaker,0.2)",
			"+I(111,scooter,Big 2-wheel scooter ,5.18)",
			"-U(110,jacket,water resistent white wind breaker,0.2)",
			"+U(110,jacket,new water resistent white wind breaker,0.5)",
			"-U(111,scooter,Big 2-wheel scooter ,5.18)",
			"+U(111,scooter,Big 2-wheel scooter ,5.17)",
			"-D(111,scooter,Big 2-wheel scooter ,5.17)",
			"-U(101,scooter,Small 2-wheel scooter,3.14)",
			"+U(101,scooter,Small 2-wheel scooter,5.17)",
			"-U(102,car battery,12V car battery,8.1)",
			"+U(102,car battery,12V car battery,5.17)",
			"-D(102,car battery,12V car battery,5.17)",
			"-D(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8)"
		);
		List<String> actual = collector.list.stream()
			.map(Object::toString)
			.collect(Collectors.toList());
		assertEquals(expected, actual);

		// test Serialization
		CanalJsonSerializationSchema serializationSchema = new CanalJsonSerializationSchema(
			SCHEMA,
			TimestampFormat.ISO_8601,
			JsonOptions.MapNullKeyMode.LITERAL,
			"null");
		serializationSchema.open(null);

		List<String> result = new ArrayList<>();
		for (RowData rowData : collector.list) {
			result.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
		}

		List<String> expectedResult = Arrays.asList("{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":1.0}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.1}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":0.5}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":5.17}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17}],\"type\":\"INSERT\"}",
			"{\"data\":[{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17}],\"type\":\"DELETE\"}",
			"{\"data\":[{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8}],\"type\":\"DELETE\"}");

		assertEquals(expectedResult, result);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static List<String> readLines(String resource) throws IOException {
		final URL url = CanalJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
		assert url != null;
		Path path = new File(url.getFile()).toPath();
		return Files.readAllLines(path);
	}

	private static class SimpleCollector implements Collector<RowData> {

		private List<RowData> list = new ArrayList<>();

		@Override
		public void collect(RowData record) {
			list.add(record);
		}

		@Override
		public void close() {
			// do nothing
		}
	}
}
