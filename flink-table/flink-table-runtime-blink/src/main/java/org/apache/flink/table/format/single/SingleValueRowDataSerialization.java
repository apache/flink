package org.apache.flink.table.format.single;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.nio.ByteBuffer;
import java.io.Serializable;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a bytes.
 */
public class SingleValueRowDataSerialization implements SerializationSchema<RowData> {

	private SerializationRuntimeConverter converter;

	private FieldGetter fieldGetter;

	public SingleValueRowDataSerialization(RowType rowType) {
		this.fieldGetter = RowData.createFieldGetter(rowType.getTypeAt(0), 0);
		this.converter = createConverter(rowType.getTypeAt(0));
	}

	@Override
	public byte[] serialize(RowData element) {
		return converter.convert(fieldGetter.getFieldOrNull(element));
	}

	/**
	 *  Runtime converter that convert a single value to byte[]
	 */
	@FunctionalInterface
	private interface SerializationRuntimeConverter extends Serializable {
		byte[] convert(Object object);
	}

	private SerializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return value -> ((StringData) value).toBytes();
			case VARBINARY:
			case BINARY:
				return value -> (byte[]) value;
			case TINYINT:
				return value -> ByteBuffer.allocate(Byte.BYTES).put((Byte) value).array();
			case SMALLINT:
				return value -> ByteBuffer.allocate(Short.BYTES).putShort((Short) value).array();
			case INTEGER:
				return value -> ByteBuffer.allocate(Integer.BYTES).putInt((Integer) value).array();
			case BIGINT:
				return value -> ByteBuffer.allocate(Long.BYTES).putLong((Long) value).array();
			case FLOAT:
				return value -> ByteBuffer.allocate(Float.BYTES).putFloat((Float) value).array();
			case DOUBLE:
				return value -> ByteBuffer.allocate(Double.BYTES).putDouble((Double) value).array();
			case BOOLEAN:
				return value -> ByteBuffer.allocate(Byte.BYTES).put(
					(byte) ((Boolean) value ? 1 : 0)).array();
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}
}
