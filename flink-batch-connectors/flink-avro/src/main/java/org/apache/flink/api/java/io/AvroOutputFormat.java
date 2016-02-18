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
package org.apache.flink.api.java.io;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.Serializable;

public class AvroOutputFormat<E> extends FileOutputFormat<E> implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Class<E> avroValueType;

	private transient Schema userDefinedSchema = null;
	
	private transient DataFileWriter<E> dataFileWriter;

	public AvroOutputFormat(Path filePath, Class<E> type) {
		super(filePath);
		this.avroValueType = type;
	}

	public AvroOutputFormat(Class<E> type) {
		this.avroValueType = type;
	}

	@Override
	protected String getDirectoryFileName(int taskNumber) {
		return super.getDirectoryFileName(taskNumber) + ".avro";
	}

	public void setSchema(Schema schema) {
		this.userDefinedSchema = schema;
	}

	@Override
	public void writeRecord(E record) throws IOException {
		dataFileWriter.append(record);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);

		DatumWriter<E> datumWriter;
		Schema schema;
		if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(avroValueType)) {
			datumWriter = new SpecificDatumWriter<E>(avroValueType);
			try {
				schema = ((org.apache.avro.specific.SpecificRecordBase)avroValueType.newInstance()).getSchema();
			} catch (InstantiationException e) {
				throw new RuntimeException(e.getMessage());
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e.getMessage());
			}
		} else {
			datumWriter = new ReflectDatumWriter<E>(avroValueType);
			schema = ReflectData.get().getSchema(avroValueType);
		}
		dataFileWriter = new DataFileWriter<E>(datumWriter);
		if (userDefinedSchema == null) {
			dataFileWriter.create(schema, stream);
		} else {
			dataFileWriter.create(userDefinedSchema, stream);
		}
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();

		if(userDefinedSchema != null) {
			byte[] json = userDefinedSchema.toString().getBytes();
			out.writeInt(json.length);
			out.write(json);
		} else {
			out.writeInt(0);
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		int length = in.readInt();
		if(length != 0) {
			byte[] json = new byte[length];
			in.readFully(json);

			Schema schema = new Schema.Parser().parse(new String(json));
			setSchema(schema);
		}
	}

	@Override
	public void close() throws IOException {
		dataFileWriter.flush();
		dataFileWriter.close();
		super.close();
	}
}
