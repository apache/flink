/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro;

//CHECKSTYLE.OFF: IllegalImport
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
//CHECKSTYLE.ON: IllegalImport

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

/**
 * The file is copied until https://github.com/confluentinc/schema-registry/issues/1432
 * is resolved.
 */
public class Schemas {

	private static final JsonFactory FACTORY = new JsonFactory();

	public static String toString(Schema schema, Collection<Schema> schemas) {
		return toString(schema, schemas, false);
	}

	public static String toString(Schema schema, Collection<Schema> schemas, boolean pretty) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = FACTORY.createJsonGenerator(writer);
			if (pretty) {
				gen.useDefaultPrettyPrinter();
			}
			Schema.Names names = new Schema.Names();
			if (schemas != null) {
				for (Schema s : schemas) {
					names.add(s);
				}
			}
			schema.toJson(names, gen);
			gen.flush();
			return writer.toString();
		} catch (IOException e) {
			throw new AvroRuntimeException(e);
		}
	}
}
