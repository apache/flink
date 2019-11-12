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

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataview.ListViewTypeInfoFactory;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ListView} provides List functionality for accumulators used by user-defined aggregate
 * functions {@link org.apache.flink.api.common.functions.AggregateFunction}.
 *
 * <p>A {@link ListView} can be backed by a Java ArrayList or a state backend, depending on the
 * context in which the aggregate function is used.
 *
 * <p>At runtime {@link ListView} will be replaced by a state ListView which is backed by a
 * {@link org.apache.flink.api.common.state.ListState} instead of {@link ArrayList} if it works
 * in streaming.
 *
 * <p>Example of an accumulator type with a {@link ListView} and an aggregate function that uses it:
 * <pre>{@code
 *
 *  public class MyAccum {
 *    public ListView<String> list;
 *    public long count;
 *  }
 *
 *  public class MyAgg extends AggregateFunction<Long, MyAccum> {
 *
 *   @Override
 *   public MyAccum createAccumulator() {
 *     MyAccum accum = new MyAccum();
 *     accum.list = new ListView<>(Types.STRING);
 *     accum.count = 0L;
 *     return accum;
 *   }
 *
 *   public void accumulate(MyAccum accumulator, String id) {
 *     accumulator.list.add(id);
 *     ... ...
 *     accumulator.get()
 *     ... ...
 *   }
 *
 *   @Override
 *   public Long getValue(MyAccum accumulator) {
 *     accumulator.list.add(id);
 *     ... ...
 *     accumulator.get()
 *     ... ...
 *     return accumulator.count;
 *   }
 * }
 *
 * }</pre>
 */
@TypeInfo(ListViewTypeInfoFactory.class)
@PublicEvolving
public class ListView<T> implements DataView {
	private static final long serialVersionUID = 5502298766901215388L;

	@Nullable
	protected transient DataType elementType;
	protected final List<T> list;

	/**
	 * @deprecated This method will be removed in the future as it is only for internal usage.
	 */
	@Deprecated
	public ListView(TypeInformation<?> elementType, List<T> list) {
		this(fromLegacyInfoToDataType(elementType), list);
	}

	/**
	 * Creates a list view for elements of the specified type.
	 *
	 * @param elementType The type of the list view elements.
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #ListView(DataType)} ()} instead which uses the new type
	 *             system based on {@link DataTypes}. See the website documentation for more information.
	 */
	@Deprecated
	public ListView(TypeInformation<?> elementType) {
		this(fromLegacyInfoToDataType(elementType), new ArrayList<>());
	}

	/**
	 * Creates a list view for elements of the specified type.
	 *
	 * @param elementType The data type of the list view elements.
	 */
	public ListView(DataType elementType) {
		this(elementType, new ArrayList<>());
	}

	/**
	 * Creates a list view.
	 */
	public ListView() {
		this((DataType) null);
	}

	/**
	 * Construct a ListView with a specified {@link List}. This is mainly used when deserialization
	 * for performance purpose. This is protected access which is only for internal usage.
	 * @param elementType The data type of the list view elements.
	 * @param list the initial list
	 */
	protected ListView(DataType elementType, List<T> list) {
		this.elementType = elementType;
		this.list = checkNotNull(list);
	}

	/**
	 * Returns an iterable of the list view.
	 *
	 * @throws Exception Thrown if the system cannot get data.
	 * @return The iterable of the list.
	 */
	public Iterable<T> get() throws Exception {
		return list;
	}

	/**
	 * Adds the given value to the list.
	 *
	 * @throws Exception Thrown if the system cannot add data.
	 * @param value The element to be appended to this list view.
	 */
	public void add(T value) throws Exception {
		list.add(value);
	}

	/**
	 * Adds all of the elements of the specified list to this list view.
	 *
	 * @throws Exception Thrown if the system cannot add all data.
	 * @param list The list with the elements that will be stored in this list view.
	 */
	public void addAll(List<T> list) throws Exception {
		this.list.addAll(list);
	}

	/**
	 * Removes the given value from the list.
	 *
	 * @param value The element to be removed from this list view.
	 */
	public boolean remove(T value) throws Exception {
		return list.remove(value);
	}

	/**
	 * Removes all of the elements from this list view.
	 */
	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ListView<?> listView = (ListView<?>) o;
		return Objects.equals(elementType, listView.elementType) &&
			Objects.equals(list, listView.list);
	}

	@Override
	public int hashCode() {
		return Objects.hash(elementType, list);
	}

}
