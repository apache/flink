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

package org.apache.flink.table.runtime.typeutils;

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
	 * Get the number of fields.
	 */
	public abstract int getArity();

	/**
	 * Convert a {@link BaseRow} to a {@link BinaryRow}.
	 */
	public abstract BinaryRow toBinaryRow(T baseRow) throws IOException;

	/**
	 * Serializes the given record to the given target paged output view. Some implementations may
	 * skip some bytes if current page does not have enough space left, .e.g {@link BinaryRow}.
	 *
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 *
	 * @return Returns the skipped number of bytes.
	 *
	 * @throws IOException Thrown, if the serialization encountered an I/O related error.
	 *                     Typically raised by the output view, which may have an underlying
	 *                     I/O channel to which it delegates.
	 */
	public abstract int serializeToPages(T record, AbstractPagedOutputView target)
			throws IOException;

	/**
	 * De-serializes a record from the given source paged input view. For consistency with serialize
	 * format, some implementations may need to skip some bytes of source before de-serializing,
	 * .e.g {@link BinaryRow}. Typically, the content read from source should be copied out when
	 * de-serializing, and we are not expecting the underlying data from source is reused. If you
	 * have such requirement, see {@link #mapFromPages(AbstractPagedInputView)}.
	 *
	 * @param source The input view from which to read the data.
	 *
	 * @return The de-serialized element.
	 *
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
	 *                     Typically raised by the input view, which may have an underlying I/O
	 *                     channel from which it reads.
	 */
	public abstract T deserializeFromPages(AbstractPagedInputView source) throws IOException;

	/**
	 * Reuse version of {@link #deserializeFromPages(AbstractPagedInputView)}.
	 */
	public abstract T deserializeFromPages(T reuse, AbstractPagedInputView source)
			throws IOException;

	/**
	 * Map a record from the given source paged input view. This method provides a possibility to
	 * achieve zero copy when de-serializing. You can either choose copy or not copy the content
	 * read from source, but we encourage to make it zero copy.
	 *
	 * <p>If you choose the zero copy way,  you have to deal with the lifecycle of the pages
	 * properly.
	 *
	 * @param source The input view from which to read the data.
	 *
	 * @return The mapped element.
	 *
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
	 *                     Typically raised by the input view, which may have an underlying I/O
	 *                     channel from which it reads.
	 */
	public abstract T mapFromPages(AbstractPagedInputView source) throws IOException;

	/**
	 * Reuse version of {@link #mapFromPages(AbstractPagedInputView)}.
	 */
	public abstract T mapFromPages(T reuse, AbstractPagedInputView source) throws IOException;

}
