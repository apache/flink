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

package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for {@link Window}.
 */
@PublicEvolving
public class WindowTypeInformation<W extends Window> extends TypeInformation<W> {

	private WindowAssigner<?, W> windowAssigner;

	public WindowTypeInformation(WindowAssigner<?, W> windowAssigner) {
		this.windowAssigner = checkNotNull(windowAssigner);
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<W> getTypeClass() {
		return new TypeHint<W>() {}.getTypeInfo().getTypeClass();
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<W> createSerializer(ExecutionConfig config) {
		return windowAssigner.getWindowSerializer(config);
	}

	@Override
	public String toString() {
		return String.format("%s(%s)", this.getClass().getSimpleName(), windowAssigner);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof WindowTypeInformation) {
			@SuppressWarnings("unchecked")
			WindowTypeInformation<W> other = (WindowTypeInformation<W>) obj;
			return other.canEqual(this) &&
				windowAssigner.equals(other.windowAssigner);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WindowTypeInformation;
	}

	@Override
	public int hashCode() {
		return windowAssigner.hashCode();
	}
}
