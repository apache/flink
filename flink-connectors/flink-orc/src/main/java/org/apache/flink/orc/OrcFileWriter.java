/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.flink.orc.OrcBatchReader.schemaToTypeInfo;

/**
 * A {@link Writer} that writes the bucket files as Hadoop {@link OrcFile}.
 *
 * @param <T> The type of the elements that are being written by the sink.
 */
public class OrcFileWriter<T extends Row> extends StreamWriterBase<T> {

	private static final long serialVersionUID = 3L;

	/**
	 * The description of the types in an ORC file.
	 */
	private TypeDescription schema;

	/**
	 * The schema of an ORC file.
	 */
	private String metaSchema;

	/**
	 * A row batch that will be written to the ORC file.
	 */
	private VectorizedRowBatch rowBatch;

	/**
	 * The writer that fill the records into the batch.
	 */
	private OrcBatchWriter orcBatchWriter;

	private transient org.apache.orc.Writer writer;

	private CompressionKind compressionKind;

	/**
	 * The number of rows that currently being written.
	 */
	private long writedRowSize;

	/**
	 * Creates a new {@code OrcFileWriter} that writes orc files without compression.
	 *
	 * @param metaSchema The orc schema.
	 */
	public OrcFileWriter(String metaSchema) {
		this(metaSchema, CompressionKind.NONE);
	}

	/**
	 * Create a new {@code OrcFileWriter} that writes orc file with the gaven
	 * schema and compression kind.
	 *
	 * @param metaSchema      The schema of an orc file.
	 * @param compressionKind The compression kind to use.
	 */
	public OrcFileWriter(String metaSchema, CompressionKind compressionKind) {
		this.metaSchema = metaSchema;
		this.schema = TypeDescription.fromString(metaSchema);
		this.compressionKind = compressionKind;
	}

	@Override
	public void open(FileSystem fs, Path path) throws IOException {
		writer = OrcFile.createWriter(path, OrcFile.writerOptions(fs.getConf()).setSchema(schema).compress(compressionKind));
		rowBatch = schema.createRowBatch();
		orcBatchWriter = new OrcBatchWriter(Arrays.asList(orcSchemaToTableSchema(schema).getTypes()));
	}

	private TableSchema orcSchemaToTableSchema(TypeDescription orcSchema) {
		List<String> fieldNames = orcSchema.getFieldNames();
		List<TypeDescription> typeDescriptions = orcSchema.getChildren();
		List<TypeInformation> typeInformations = new ArrayList<>();

		typeDescriptions.forEach(typeDescription -> {
			typeInformations.add(schemaToTypeInfo(typeDescription));
		});

		return new TableSchema(
			fieldNames.toArray(new String[fieldNames.size()]),
			typeInformations.toArray(new TypeInformation[typeInformations.size()]));
	}

	@Override
	public void write(T element) throws IOException {
		Boolean isFill = orcBatchWriter.fill(rowBatch, element);
		if (!isFill) {
			writer.addRowBatch(rowBatch);
			writedRowSize += writedRowSize + rowBatch.size;
			rowBatch.reset();
		}
	}

	@Override
	public long flush() throws IOException {
		writer.addRowBatch(rowBatch);
		writedRowSize += rowBatch.size;
		rowBatch.reset();
		return writedRowSize;
	}

	@Override
	public long getPos() throws IOException {
		return writedRowSize;
	}

	@Override
	public Writer<T> duplicate() {
		return new OrcFileWriter<>(metaSchema, compressionKind);
	}

	@Override
	public void close() throws IOException {
		flush();
		if (rowBatch.size != 0) {
			writer.addRowBatch(rowBatch);
			rowBatch.reset();
		}
		writer.close();
		super.close();
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), schema, metaSchema, rowBatch, orcBatchWriter);
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null) {
			return false;
		}
		if (getClass() != other.getClass()) {
			return false;
		}
		OrcFileWriter<T> writer = (OrcFileWriter<T>) other;
		return Objects.equals(schema, writer.schema)
			&& Objects.equals(metaSchema, writer.metaSchema)
			&& Objects.equals(rowBatch, writer.rowBatch)
			&& Objects.equals(orcBatchWriter, writer.orcBatchWriter);
	}

	/**
	 * The writer that fill the records into the batch.
	 */
	private class OrcBatchWriter {

		private List<TypeInformation> typeInfos;

		public OrcBatchWriter(List<TypeInformation> typeInfos) {
			this.typeInfos = typeInfos;
		}

		/**
		 * Fill the record into {@code VectorizedRowBatch}, return false if batch is full.
		 *
		 * @param batch  An reusable orc VectorizedRowBatch.
		 * @param record Input record.
		 * @return Return false if batch is full.
		 */
		private boolean fill(VectorizedRowBatch batch, T record) {
			// If the batch is full, write it out and start over.
			if (batch.size == batch.getMaxSize()) {
				return false;
			} else {
				IntStream.range(0, typeInfos.size()).forEach(
					index -> {
						setColumnVectorValueByType(typeInfos.get(index), batch, index, batch.size, record);
					}
				);
				batch.size += 1;
				return true;
			}
		}

		private void setColumnVectorValueByType(TypeInformation typeInfo,
												VectorizedRowBatch batch,
												int index,
												int nextPosition,
												T record) {
			switch (typeInfo.toString()) {
				case "Long":
					LongColumnVector longColumnVector = (LongColumnVector) batch.cols[index];
					longColumnVector.vector[nextPosition] = (Long) record.getField(index);
					break;
				case "Boolean":
					LongColumnVector booleanColumnVector = (LongColumnVector) batch.cols[index];
					Boolean bool = (Boolean) record.getField(index);
					int boolValue;
					if (bool) {
						boolValue = 1;
					} else {
						boolValue = 0;
					}
					booleanColumnVector.vector[nextPosition] = boolValue;
					break;
				case "Short":
					LongColumnVector shortColumnVector = (LongColumnVector) batch.cols[index];
					shortColumnVector.vector[nextPosition] = (Short) record.getField(index);
					break;
				case "Integer":
					LongColumnVector intColumnVector = (LongColumnVector) batch.cols[index];
					intColumnVector.vector[nextPosition] = (Integer) record.getField(index);
					break;
				case "Float":
					DoubleColumnVector floatColumnVector = (DoubleColumnVector) batch.cols[index];
					floatColumnVector.vector[nextPosition] = (Float) record.getField(index);
					break;
				case "Double":
					DoubleColumnVector doubleColumnVector = (DoubleColumnVector) batch.cols[index];
					doubleColumnVector.vector[nextPosition] = (Double) record.getField(index);
					break;
				case "String":
					BytesColumnVector stringColumnVector = (BytesColumnVector) batch.cols[index];
					stringColumnVector.setVal(nextPosition, ((String) record.getField(index)).getBytes(StandardCharsets.UTF_8));
					break;
				default:
					throw new IllegalArgumentException("Unsupported column type " + typeInfo);
			}
		}
	}
}
