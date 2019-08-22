/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.api.common.serialization;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class SerializationEncoder<IN> extends SerializationEncoderBase<IN> {

	private static final long serialVersionUID = 5931653074836191062L;

	public SerializationEncoder(SerializationSchema serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	@Override
	public void encode(IN element, OutputStream stream) throws IOException {
		byte[] b = serializationSchema.serialize(element);

		if (b != null) {
			//Data in csv format contains  line  delimiter (default is \n), So we no longer write line  delimiter .
			if ((b.length > 0 && (b[b.length - 1] == '\n' || b[b.length - 1] == '\r')) || (
				b.length > 1 && b[b.length - 1] == '\n' && b[b.length - 2] == '\r')) {
				stream.write(b);
			} else {
				stream.write(b);
				stream.write(System.lineSeparator().getBytes());
			}

		}
	}
}
