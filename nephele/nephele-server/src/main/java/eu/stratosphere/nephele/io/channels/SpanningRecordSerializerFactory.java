/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import eu.stratosphere.nephele.types.Record;

/**
 * This class implements a factory for the {@link SpanningRecordSerializer}, a special serializer for records which are
 * too big to fit into a single {@link Buffer} at runtime.
 * 
 * @author warneke
 * @param <T>
 *        the type of record to be serialized by the record serializer
 */
public final class SpanningRecordSerializerFactory<T extends Record> implements RecordSerializerFactory<T> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RecordSerializer<T> createSerializer() {
		return new SpanningRecordSerializer<T>();
	}

}
