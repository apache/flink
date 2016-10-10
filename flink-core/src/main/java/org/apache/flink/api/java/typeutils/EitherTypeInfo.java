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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.types.Either;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for the {@link Either} type of the Java API.
 *
 * @param <L> the Left value type
 * @param <R> the Right value type
 */
@Public
public class EitherTypeInfo<L, R> extends TypeInformation<Either<L, R>> {

	private static final long serialVersionUID = 1L;

	private final TypeInformation<L> leftType;

	private final TypeInformation<R> rightType;

	@PublicEvolving
	public EitherTypeInfo(TypeInformation<L> leftType, TypeInformation<R> rightType) {
		this.leftType = checkNotNull(leftType);
		this.rightType = checkNotNull(rightType);
	}

	@Override
	@PublicEvolving
	public boolean isBasicType() {
		return false;
	}

	@Override
	@PublicEvolving
	public boolean isTupleType() {
		return false;
	}

	@Override
	@PublicEvolving
	public int getArity() {
		return 1;
	}

	@Override
	@PublicEvolving
	public int getTotalFields() {
		return 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	@PublicEvolving
	public Class<Either<L, R>> getTypeClass() {
		return (Class<Either<L, R>>) (Class<?>) Either.class;
	}

	@Override
	@PublicEvolving
	public Map<String, TypeInformation<?>> getGenericParameters() {
		Map<String, TypeInformation<?>> m = new HashMap<>();
		m.put("L", this.leftType);
		m.put("R", this.rightType);
		return m;
	}

	@Override
	@PublicEvolving
	public boolean isKeyType() {
		return false;
	}

	@Override
	@PublicEvolving
	public TypeSerializer<Either<L, R>> createSerializer(ExecutionConfig config) {
		return new EitherSerializer<L, R>(leftType.createSerializer(config),
				rightType.createSerializer(config));
	}

	@Override
	public String toString() {
		return "Either <" + leftType.toString() + ", " + rightType.toString() + ">";
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EitherTypeInfo) {
			EitherTypeInfo<L, R> other = (EitherTypeInfo<L, R>) obj;

			return other.canEqual(this) &&
				leftType.equals(other.leftType) &&
				rightType.equals(other.rightType);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 17 * leftType.hashCode() + rightType.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof EitherTypeInfo;
	}

	// --------------------------------------------------------------------------------------------

	public TypeInformation<L> getLeftType() {
		return leftType;
	}

	public TypeInformation<R> getRightType() {
		return rightType;
	}

}
