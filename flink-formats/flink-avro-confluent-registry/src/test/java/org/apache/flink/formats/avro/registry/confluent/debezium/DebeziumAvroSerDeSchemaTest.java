package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DebeziumAvroDeserializationSchema}.
 */
public class DebeziumAvroSerDeSchemaTest {

	private static final String SUBJECT = "testDebeziumAvro";

	private static SchemaRegistryClient client = new MockSchemaRegistryClient();

	private static final RowType rowType = (RowType) ROW(
		FIELD("id", BIGINT()),
		FIELD("name", STRING()),
		FIELD("description", STRING()),
		FIELD("weight", DOUBLE())
	).getLogicalType();

	private static final Schema DEBEZIUM_SCHEMA_COMPATIBLE_TEST = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"fullfillment.test1.person\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"description\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"weight\",\"type\":[\"null\",\"double\"],\"default\":null}],\"connect.name\":\"fullfillment.test1.person.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"ConnectDefault\",\"namespace\":\"io.confluent.connect.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}]}],\"default\":null}],\"connect.name\":\"fullfillment.test1.person.Envelope\"}");

	@Test
	public void testSerializationDeserialization() throws Exception {

		RowType rowTypeDe = DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(fromLogicalToDataType(rowType));
		RowType rowTypeSe = DebeziumAvroSerializationSchema.createDebeziumAvroRowType(fromLogicalToDataType(rowType));

		AvroRowDataSerializationSchema serializer = getSerializationSchema(rowTypeSe, DEBEZIUM_SCHEMA_COMPATIBLE_TEST);
		DebeziumAvroSerializationSchema debeziumAvroSerializationSchema = new DebeziumAvroSerializationSchema(serializer);
		serializer.open(null);

		byte[] serialize = debeziumAvroSerializationSchema.serialize(debeziumRow2RowData());

		client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST);
		AvroRowDataDeserializationSchema deserializer = getDeserializationSchema(rowTypeDe, DEBEZIUM_SCHEMA_COMPATIBLE_TEST);
		DebeziumAvroDeserializationSchema debeziumAvroDeserializationSchema = new DebeziumAvroDeserializationSchema(rowTypeDe, InternalTypeInfo.of(rowType), deserializer);

		deserializer.open(null);
		SimpleCollector collector = new SimpleCollector();

		debeziumAvroDeserializationSchema.deserialize(serialize, collector);

		List<String> actual = collector.list.stream()
			.map(Object::toString)
			.collect(Collectors.toList());

		List<String> expected = Arrays.asList(
			"+I(107,rocks,box of assorted rocks,5.3)");
		assertEquals(expected, actual);
	}

	@Test
	public void testInsertDataDeserialization() throws Exception {
		List<String> actual = testDeserialization(getPath("debezium-avro-insert.txt"));

		List<String> expected = Arrays.asList(
			"+I(1,lisi,test debezium avro data,21.799999237060547)");
		assertEquals(expected, actual);
	}

	@Test
	public void testUpdateDataDeserialization() throws Exception {
		List<String> actual = testDeserialization(getPath("debezium-avro-update.txt"));

		List<String> expected = Arrays.asList(
			"-U(1,lisi,test debezium avro data,21.799999237060547)",
			"+U(1,zhangsan,test debezium avro data,21.799999237060547)");
		assertEquals(expected, actual);
	}

	@Test
	public void testDeleteDataDeserialization() throws Exception {
		List<String> actual = testDeserialization(getPath("debezium-avro-delete.txt"));

		List<String> expected = Arrays.asList(
			"-D(1,zhangsan,test debezium avro data,21.799999237060547)");
		assertEquals(expected, actual);
	}

	public List<String> testDeserialization(Path path) throws Exception {
		RowType rowTypeDe = DebeziumAvroDeserializationSchema
			.createDebeziumAvroRowType(fromLogicalToDataType(rowType));

		client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST, 1, 81);

		AvroRowDataDeserializationSchema deserializer = getDeserializationSchema(rowTypeDe, DEBEZIUM_SCHEMA_COMPATIBLE_TEST);
		DebeziumAvroDeserializationSchema debeziumAvroDeserializationSchema = new DebeziumAvroDeserializationSchema(rowTypeDe, InternalTypeInfo.of(rowType), deserializer);

		deserializer.open(null);
		SimpleCollector collector = new SimpleCollector();
		byte[] bytes = FileUtils.readAllBytes(path);

		debeziumAvroDeserializationSchema.deserialize(bytes, collector);

		List<String> actual = collector.list.stream()
			.map(Object::toString)
			.collect(Collectors.toList());

		return actual;
	}

	private Path getPath(String filePath) {
		URL url = DebeziumAvroSerDeSchemaTest.class.getClassLoader().getResource(filePath);
		return new File(url.getFile()).toPath();
	}

	private static AvroRowDataDeserializationSchema getDeserializationSchema(RowType rowType, Schema schema) {

		ConfluentSchemaRegistryCoder registryCoder = new ConfluentSchemaRegistryCoder(SUBJECT, client);

		return new AvroRowDataDeserializationSchema(
			new RegistryAvroDeserializationSchema<>(
				GenericRecord.class,
				AvroSchemaConverter.convertToSchema(rowType),
				() -> registryCoder
			),
			AvroToRowDataConverters.createRowConverter(rowType),
			InternalTypeInfo.of(rowType));
	}

	private static AvroRowDataSerializationSchema getSerializationSchema(RowType rowType, Schema schema) {

		ConfluentSchemaRegistryCoder registryCoder = new ConfluentSchemaRegistryCoder(SUBJECT, client);
		return new AvroRowDataSerializationSchema(
			rowType,
			new RegistryAvroSerializationSchema<>(
				GenericRecord.class,
				AvroSchemaConverter.convertToSchema(rowType),
				() -> registryCoder),
			RowDataToAvroConverters.createRowConverter(rowType));
	}

	private static RowData debeziumRow2RowData() {
		GenericRowData rowData = new GenericRowData(4);
		rowData.setField(0, 107L);
		rowData.setField(1, new BinaryStringData("rocks"));
		rowData.setField(2, new BinaryStringData("box of assorted rocks"));
		rowData.setField(3, 5.3D);
		return rowData;
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
