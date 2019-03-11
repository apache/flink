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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;

import java.io.IOException;

/**
 * Row serializer, provided paged serialize paged method.
 */
public abstract class AbstractRowSerializer<T extends BaseRow> extends TypeSerializer<T> {

	/**
	 * Convert a {@link BaseRow} to a {@link BinaryRow}.
	 */
	public abstract BinaryRow baseRowToBinary(T baseRow) throws IOException;

	/**
	 * Serializes the given record to the given target paged output view. Make specific implementers decide whether
	 * or not to jump, how many positions jump. If jumps, will skip some offset.
	 *
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 * @return Returns how much offset is skipped.
	 * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the
	 *                     output view, which may have an underlying I/O channel to which it delegates.
	 */
	public int serializeToPages(T record, AbstractPagedOutputView target) throws IOException {
		serialize(record, target);
		return 0;
	}

	/**
	 * De-serializes a record from the given source paged input view. Make specific implementers decide whether
	 * or not to jump, how many positions jump. If jumps, will skip some bytes to read.
	 *
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 *
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public T deserializeFromPages(AbstractPagedInputView source) throws IOException {
		return deserialize(source);
	}

	/**
	 * Reuse version of {@link #deserializeFromPages(AbstractPagedInputView)}.
	 */
	public T deserializeFromPages(T reuse, AbstractPagedInputView source) throws IOException {
		return deserialize(reuse, source);
	}

	/**
	 * Map a record from the given source paged input view. This method provides a possibility to achieve zero copy,
	 * you can copy, you can not copy, while you have to properly manage the life cycle of the page.
	 * Pay attention: Before the end of the visit(Or iterator), pages can not be released.
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 *
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public T mapFromPages(AbstractPagedInputView source) throws IOException {
		return deserialize(source);
	}

	/**
	 * Reuse version of {@link #mapFromPages(AbstractPagedInputView)}.
	 */
	public T mapFromPages(T reuse, AbstractPagedInputView source) throws IOException {
		return deserialize(reuse, source);
	}
}
