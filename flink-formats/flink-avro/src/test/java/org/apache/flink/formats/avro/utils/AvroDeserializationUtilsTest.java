package org.apache.flink.formats.avro.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for AvroDeserializationUtils.
 */
public class AvroDeserializationUtilsTest {

	@Test
	public void testConvertAvroRecordToRow() {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		Class<? extends SpecificRecord> recordClass = testData.f0;
		SpecificRecord record = testData.f1;
		Row recordRow = testData.f2;
		Schema schema = testData.f1.getSchema();

		TypeInformation<Row> rowTypeInfo = AvroSchemaConverter.convertToTypeInfo(recordClass);
		Row actual = AvroDeserializationUtils.convertAvroRecordToRow(schema, (RowTypeInfo) rowTypeInfo, record);

		assertEquals(recordRow, actual);
	}
}
