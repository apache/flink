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
package org.apache.flink.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;


/**
 * An {@link OutputTag} is a typed and named tag to use for tagging side outputs
 * of an operator.
 *
 * <p>An {@code OutputTag} must always be an anonymous inner class so that Flink can derive
 * a {@link TypeInformation} for the generic type parameter.
 *
 * <p>Example:
 * <pre>{@code
 * OutputTag<Tuple2<String, Long>> info = new OutputTag<Tuple2<String, Long>>("late-data"){});
 * }</pre>
 *
 * @param <T> the type of elements in the side-output stream.
 */
@PublicEvolving
public class OutputTag<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String id;

	private transient TypeInformation<T> typeInfo;

	/**
	 * Creates a new named {@code OutputTag} with the given id.
	 *
	 * @param id The id of the created {@code OutputTag}.
     */
	public OutputTag(String id) {
		this.id = Preconditions.checkNotNull(id, "OutputTag id cannot be null.");

		try {
			TypeHint<T> typeHint = new TypeHint<T>(OutputTag.class, this, 0) {};
			this.typeInfo = typeHint.getTypeInfo();
		} catch (InvalidTypesException e) {
			throw new InvalidTypesException("Could not determine TypeInformation for generic " +
					"OutputTag type. Did you forget to make your OutputTag an anonymous inner class?", e);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		typeInfo = null;
	}

	public String getId() {
		return id;
	}

	public TypeInformation<T> getTypeInfo() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof OutputTag
			&& ((OutputTag) obj).id.equals(this.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return "OutputTag(" + getTypeInfo() + ", " + id + ")";
	}
}
