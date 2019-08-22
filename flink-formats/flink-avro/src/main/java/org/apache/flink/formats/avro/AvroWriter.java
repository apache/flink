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

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;

import org.apache.avro.file.DataFileWriter;

import java.io.IOException;
import java.util.TimeZone;

/**
 * A  implementation .
 *
 * @param <T> The type of written.
 */
@PublicEvolving
public class AvroWriter<T> implements BulkWriter<T> {

	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();
	private final DataFileWriter dataFileWriter;
	private String schemaStr;

	public AvroWriter(DataFileWriter dataFileWriter, String schemaStr) {
		this.dataFileWriter = dataFileWriter;
		this.schemaStr = schemaStr;
	}

	@Override
	public void addElement(T element) throws IOException {
		dataFileWriter.append(element);
	}

	@Override
	public void flush() throws IOException {
		// the dataFileWriter flush contain flush and close ，so we do nothing in finish() .
		dataFileWriter.flush();
	}

	@Override
	public void finish() throws IOException {
//		do nothing
	}
}
