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


import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

public class CsvInputFormat<OUT> extends CommonCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;
	
	public CsvInputFormat(Path filePath, CompositeType<OUT> typeInformation) {
		super(filePath, typeInformation);
	}
	
	public CsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, CompositeType<OUT> typeInformation) {
		super(filePath, lineDelimiter, fieldDelimiter, typeInformation);
	}

	@Override
	protected OUT createTuple(OUT reuse) {
		Tuple result = (Tuple) reuse;
		for (int i = 0; i < parsedValues.length; i++) {
			result.setField(parsedValues[i], i);
		}

		return reuse;
	}

	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + getFilePath();
	}
	
}
