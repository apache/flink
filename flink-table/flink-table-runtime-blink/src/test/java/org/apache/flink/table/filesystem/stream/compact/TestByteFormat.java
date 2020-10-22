package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;

/**
 * Test byte stream format.
 */
public class TestByteFormat extends SimpleStreamFormat<Byte> {

	@Override
	public Reader<Byte> createReader(Configuration config,
			FSDataInputStream stream) {
		return new Reader<Byte>() {
			@Override
			public Byte read() throws IOException {
				byte b = (byte) stream.read();
				if (b == -1) {
					return null;
				}
				return b;
			}

			@Override
			public void close() throws IOException {
				stream.close();
			}
		};
	}

	@Override
	public TypeInformation<Byte> getProducedType() {
		return BasicTypeInfo.BYTE_TYPE_INFO;
	}

	public static BulkFormat<Byte> bulkFormat() {
		return new StreamFormatAdapter<>(new TestByteFormat());
	}
}
