package org.apache.flink.table.format.single;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.Serializable;


/**
 * Deserialization schema from SINGLE-VALUE to Flink Table/SQL internal data structure {@link RowData}.
 */
public class SingleValueRowDataDeserialization implements DeserializationSchema<RowData> {

	private DeserializationRuntimeConverter converter;
	private TypeInformation<RowData> typeInfo;

	public SingleValueRowDataDeserialization(RowType rowType,
		TypeInformation<RowData> resultTypeInfo) {
		this.typeInfo = resultTypeInfo;
		this.converter = createConverter(rowType.getTypeAt(0));
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		GenericRowData genericRowData = new GenericRowData(1);
		genericRowData.setField(0, converter.convert(message));
		return genericRowData;
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return typeInfo;
	}

	/**
	 * Runtime converter that convert byte[] to a single value
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(byte[] message);
	}

	/**
	 *  Creates a runtime converter.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return bytes -> StringData.fromBytes(bytes);
			case VARBINARY:
			case BINARY:
				return bytes -> bytes;
			case TINYINT:
				return bytes -> ByteBuffer.wrap(bytes).get();
			case SMALLINT:
				return bytes -> ByteBuffer.wrap(bytes).getShort() ;
			case INTEGER:
				return bytes -> ByteBuffer.wrap(bytes).getInt();
			case BIGINT:
				return bytes -> ByteBuffer.wrap(bytes).getLong();
			case FLOAT:
				return bytes -> ByteBuffer.wrap(bytes).getFloat();
			case DOUBLE:
				return bytes -> ByteBuffer.wrap(bytes).getDouble();
			case BOOLEAN:
				return bytes -> ByteBuffer.wrap(bytes).get() != 0;
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}
}
