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
package org.apache.flink.api.common.typeinfo;

import static java.util.Objects.requireNonNull;
import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;


/**
 * Sideoutput meta info, used to split sideoutput next to operator
 */
@PublicEvolving
public abstract class OutputTag<T> extends TypeHint<T> implements Serializable{

	private static final long serialVersionUID = 1L;

	private final T value;

	/**
	 * reserved constructor for {@code LateArrivingOutputTag}
	 */
	protected OutputTag() {
		this.value = null;
	}

	/**
	 * assign value of typed outputtag
	 * allow expose more than one outputtag with same type
	 * @param value outputtag value
     */
	public OutputTag(T value) {
		this.value = requireNonNull(value);
	}

	/**
	 * get value of outputtag
	 * @return outputtag value
     */
	public T getValue() {
		return value;
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof OutputTag
			&& ((OutputTag)obj).getTypeInfo().equals(this.getTypeInfo())
			&& (((OutputTag)obj).value != null && ((OutputTag)obj).value.equals(this.value)
				|| (((OutputTag)obj).value == null && this.value == null));
	}

	@Override
	public int hashCode() {
		int result = value != null ? value.hashCode() : 0;
		return 31 * result + (getTypeInfo().hashCode() ^ (getTypeInfo().hashCode() >>> 32));
	}

	@Override
	public String toString() {
		return getTypeInfo().toString() + "@" + (value == null ? "undefined" : value.toString());
	}
}
