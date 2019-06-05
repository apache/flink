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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical type of a user-defined object structured type. Structured types contain one or more
 * attributes. Each attribute consists of a name and a type. A type cannot be defined so that one of
 * its attribute types (transitively) uses itself.
 *
 * <p>A structured type can declare a super type and allows single inheritance for more complex type
 * hierarchies, similar to JVM-based languages.
 *
 * <p>A structured type must be declared {@code final} for preventing further inheritance (default
 * behavior) or {@code not final} for allowing subtypes.
 *
 * <p>A structured type must be declared {@code not instantiable} if a more specific type is
 * required or {@code instantiable} if instances can be created from this type (default behavior).
 *
 * <p>A structured type declares comparision properties of either {@code none} (no equality),
 * {@code equals} (only equality and inequality), or {@code full} (greater, equals, less).
 *
 * <p>NOTE: Compared to the SQL standard, this class is incomplete. We might add new features such
 * as method declarations in the future. Also ordering is not supported yet.
 */
@PublicEvolving
public final class StructuredType extends UserDefinedType {

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		Row.class.getName(),
		"org.apache.flink.table.dataformat.BaseRow");

	private static final Class<?> FALLBACK_CONVERSION = Row.class;

	/**
	 * Defines an attribute of a {@link StructuredType}.
	 */
	public static final class StructuredAttribute implements Serializable {

		private final String name;

		private final LogicalType type;

		private final @Nullable String description;

		public StructuredAttribute(String name, LogicalType type, @Nullable String description) {
			this.name = Preconditions.checkNotNull(name, "Attribute name must not be null.");
			this.type = Preconditions.checkNotNull(type, "Attribute type must not be null.");
			this.description = description;
		}

		public StructuredAttribute(String name, LogicalType type) {
			this(name, type, null);
		}

		public String getName() {
			return name;
		}

		public LogicalType getType() {
			return type;
		}

		public Optional<String> getDescription() {
			return Optional.ofNullable(description);
		}

		public StructuredAttribute copy() {
			return new StructuredAttribute(name, type.copy(), description);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			StructuredAttribute that = (StructuredAttribute) o;
			return name.equals(that.name) &&
				type.equals(that.type) &&
				Objects.equals(description, that.description);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, type, description);
		}
	}

	/**
	 * Defines equality properties for scalar evaluation.
	 */
	public enum StructuredComparision {
		EQUALS,
		FULL,
		NONE
	}

	/**
	 * A builder for a {@link StructuredType}. Intended for future extensibility.
	 */
	public static class Builder {

		private final ObjectIdentifier objectIdentifier;

		private final List<StructuredAttribute> attributes;

		private boolean isNullable = true;

		private boolean isFinal = true;

		private boolean isInstantiable = true;

		private StructuredComparision comparision = StructuredComparision.NONE;

		private @Nullable StructuredType superType;

		private @Nullable String description;

		private @Nullable Class<?> implementationClass;

		public Builder(ObjectIdentifier objectIdentifier, List<StructuredAttribute> attributes) {
			this.objectIdentifier = Preconditions.checkNotNull(objectIdentifier, "Object identifier must not be null.");
			this.attributes = Collections.unmodifiableList(
				new ArrayList<>(
					Preconditions.checkNotNull(attributes, "Attributes must not be null.")));

			Preconditions.checkArgument(
				attributes.size() > 0,
				"Attribute list must not be empty.");
		}

		public Builder setNullable(boolean isNullable) {
			this.isNullable = isNullable;
			return this;
		}

		public Builder setDescription(String description) {
			this.description = Preconditions.checkNotNull(description, "Description must not be null.");
			return this;
		}

		public Builder setFinal(boolean isFinal) {
			this.isFinal = isFinal;
			return this;
		}

		public Builder setInstantiable(boolean isInstantiable) {
			this.isInstantiable = isInstantiable;
			return this;
		}

		public Builder setComparision(StructuredComparision comparision) {
			this.comparision = Preconditions.checkNotNull(comparision, "Comparision must not be null.");
			return this;
		}

		public Builder setSuperType(@Nullable StructuredType superType) {
			this.superType = Preconditions.checkNotNull(superType, "Super type must not be null.");
			return this;
		}

		public Builder setImplementationClass(Class<?> implementationClass) {
			this.implementationClass = Preconditions.checkNotNull(implementationClass, "Implementation class must not null.");
			return this;
		}

		public StructuredType build() {
			return new StructuredType(
				isNullable,
				objectIdentifier,
				attributes,
				isFinal,
				isInstantiable,
				comparision,
				superType,
				description,
				implementationClass);
		}
	}

	private final List<StructuredAttribute> attributes;

	private final boolean isInstantiable;

	private final StructuredComparision comparision;

	private final @Nullable StructuredType superType;

	private final @Nullable Class<?> implementationClass;

	private StructuredType(
			boolean isNullable,
			ObjectIdentifier objectIdentifier,
			List<StructuredAttribute> attributes,
			boolean isFinal,
			boolean isInstantiable,
			StructuredComparision comparision,
			@Nullable StructuredType superType,
			@Nullable String description,
			@Nullable Class<?> implementationClass) {
		super(
			isNullable,
			LogicalTypeRoot.STRUCTURED_TYPE,
			objectIdentifier,
			isFinal,
			description);

		this.attributes = attributes;
		this.isInstantiable = isInstantiable;
		this.comparision = comparision;
		this.superType = superType;
		this.implementationClass = implementationClass;
	}

	public List<StructuredAttribute> getAttributes() {
		return attributes;
	}

	public boolean isInstantiable() {
		return isInstantiable;
	}

	public StructuredComparision getComparision() {
		return comparision;
	}

	public Optional<StructuredType> getSuperType() {
		return Optional.ofNullable(superType);
	}

	public Optional<Class<?>> getImplementationClass() {
		return Optional.ofNullable(implementationClass);
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new StructuredType(
			isNullable,
			getObjectIdentifier(),
			attributes.stream().map(StructuredAttribute::copy).collect(Collectors.toList()),
			isFinal(),
			isInstantiable,
			comparision,
			superType == null ? null : (StructuredType) superType.copy(),
			getDescription().orElse(null),
			implementationClass);
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return (implementationClass != null && implementationClass.isAssignableFrom(clazz)) ||
			INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		StructuredType currentType = this;
		while (currentType != null) {
			if (currentType.implementationClass != null && clazz.isAssignableFrom(currentType.implementationClass)) {
				return true;
			}
			currentType = currentType.superType;
		}
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultConversion() {
		if (implementationClass != null) {
			return implementationClass;
		}
		return FALLBACK_CONVERSION;
	}

	@Override
	public List<LogicalType> getChildren() {
		final ArrayList<LogicalType> children = new ArrayList<>();
		StructuredType currentType = this;
		while (currentType != null) {
			children.addAll(
				currentType.attributes.stream()
					.map(StructuredAttribute::getType)
					.collect(Collectors.toList()));
			currentType = currentType.superType;
		}
		Collections.reverse(children);
		return Collections.unmodifiableList(children);
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		StructuredType that = (StructuredType) o;
		return isInstantiable == that.isInstantiable &&
			attributes.equals(that.attributes) &&
			comparision == that.comparision &&
			Objects.equals(superType, that.superType) &&
			Objects.equals(implementationClass, that.implementationClass);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			super.hashCode(),
			attributes,
			isInstantiable,
			comparision,
			superType,
			implementationClass);
	}
}
