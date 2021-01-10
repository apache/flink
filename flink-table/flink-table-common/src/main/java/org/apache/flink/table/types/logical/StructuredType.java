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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
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
 * Logical type of a user-defined object structured type. Structured types contain zero, one or more
 * attributes. Each attribute consists of a name and a type. A type cannot be defined so that one of
 * its attribute types (transitively) uses itself.
 *
 * <p>There are two kinds of structured types. Types that are stored in a catalog and are identified
 * by an {@link ObjectIdentifier} or anonymously defined, unregistered types (usually reflectively
 * extracted) that are identified by an implementation {@link Class}.
 *
 * <h1>Logical properties</h1>
 *
 * <p>A structured type can declare a super type and allows single inheritance for more complex type
 * hierarchies, similar to JVM-based languages.
 *
 * <p>A structured type can be declared {@code final} for preventing further inheritance (default
 * behavior) or {@code not final} for allowing subtypes.
 *
 * <p>A structured type can be declared {@code not instantiable} if a more specific type is required
 * or {@code instantiable} if instances can be created from this type (default behavior).
 *
 * <p>A structured type declares comparision properties of either {@code none} (no equality), {@code
 * equals} (only equality and inequality), or {@code full} (greater, equals, less).
 *
 * <p>NOTE: Compared to the SQL standard, this class is incomplete. We might add new features such
 * as method declarations in the future. Also ordering is not supported yet.
 *
 * <h1>Physical properties</h1>
 *
 * <p>A structured type can be defined fully logically (e.g. by using a {@code CREATE TYPE} DDL).
 * The implementation class is optional and only used at the edges of the table ecosystem (e.g. when
 * bridging to a function or connector). Serialization and equality ({@code hashCode/equals}) are
 * handled by the runtime based on the logical type. In other words: {@code hashCode/equals} of an
 * implementation class are not used. Custom equality, casting logic, and further overloaded
 * operators will be supported once we allow defining methods on structured types.
 *
 * <p>An implementation class must offer a default constructor with zero arguments or a full
 * constructor that assigns all attributes. Other physical properties such as the conversion classes
 * of attributes are defined by a {@link DataType} when a structured type is used.
 */
@PublicEvolving
public final class StructuredType extends UserDefinedType {

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(Row.class.getName(), RowData.class.getName());

    private static final Class<?> FALLBACK_CONVERSION = Row.class;

    /** Defines an attribute of a {@link StructuredType}. */
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
            return name.equals(that.name)
                    && type.equals(that.type)
                    && Objects.equals(description, that.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, description);
        }
    }

    /** Defines equality properties for scalar evaluation. */
    public enum StructuredComparision {
        EQUALS(true, false),
        FULL(true, true),
        NONE(false, false);
        private final boolean equality;
        private final boolean comparison;

        StructuredComparision(boolean equality, boolean comparison) {
            this.equality = equality;
            this.comparison = comparison;
        }

        public boolean isEquality() {
            return equality;
        }

        public boolean isComparison() {
            return comparison;
        }
    }

    /** A builder for a {@link StructuredType}. Intended for future extensibility. */
    public static final class Builder {

        private final @Nullable ObjectIdentifier objectIdentifier;

        private final @Nullable Class<?> implementationClass;

        private List<StructuredAttribute> attributes = new ArrayList<>();

        private boolean isNullable = true;

        private boolean isFinal = true;

        private boolean isInstantiable = true;

        private StructuredComparision comparision = StructuredComparision.NONE;

        private @Nullable StructuredType superType;

        private @Nullable String description;

        public Builder(Class<?> implementationClass) {
            this.objectIdentifier = null;
            this.implementationClass =
                    Preconditions.checkNotNull(
                            implementationClass, "Implementation class must not be null.");
        }

        public Builder(ObjectIdentifier objectIdentifier) {
            this.objectIdentifier =
                    Preconditions.checkNotNull(
                            objectIdentifier, "Object identifier must not be null.");
            this.implementationClass = null;
        }

        public Builder(ObjectIdentifier objectIdentifier, Class<?> implementationClass) {
            this.objectIdentifier =
                    Preconditions.checkNotNull(
                            objectIdentifier, "Object identifier must not be null.");
            this.implementationClass =
                    Preconditions.checkNotNull(
                            implementationClass, "Implementation class must not be null.");
        }

        public Builder attributes(List<StructuredAttribute> attributes) {
            this.attributes =
                    Collections.unmodifiableList(
                            new ArrayList<>(
                                    Preconditions.checkNotNull(
                                            attributes, "Attributes must not be null.")));
            return this;
        }

        public Builder setNullable(boolean isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public Builder description(String description) {
            this.description =
                    Preconditions.checkNotNull(description, "Description must not be null.");
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

        public Builder comparision(StructuredComparision comparision) {
            this.comparision =
                    Preconditions.checkNotNull(comparision, "Comparision must not be null.");
            return this;
        }

        public Builder superType(StructuredType superType) {
            this.superType = Preconditions.checkNotNull(superType, "Super type must not be null.");
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
        super(isNullable, LogicalTypeRoot.STRUCTURED_TYPE, objectIdentifier, isFinal, description);

        Preconditions.checkArgument(
                objectIdentifier != null || implementationClass != null,
                "An identifier is missing.");

        this.attributes = attributes;
        this.isInstantiable = isInstantiable;
        this.comparision = comparision;
        this.superType = superType;
        this.implementationClass = implementationClass;
    }

    /**
     * Creates a builder for a {@link StructuredType} that has been stored in a catalog and is
     * identified by an {@link ObjectIdentifier}.
     */
    public static StructuredType.Builder newBuilder(ObjectIdentifier objectIdentifier) {
        return new StructuredType.Builder(objectIdentifier);
    }

    /**
     * Creates a builder for a {@link StructuredType} that has been stored in a catalog and is
     * identified by an {@link ObjectIdentifier}. The optional implementation class defines
     * supported conversions.
     */
    public static StructuredType.Builder newBuilder(
            ObjectIdentifier objectIdentifier, Class<?> implementationClass) {
        return new StructuredType.Builder(objectIdentifier, implementationClass);
    }

    /**
     * Creates a builder for a {@link StructuredType} that is not stored in a catalog and is
     * identified by an implementation {@link Class}.
     */
    public static StructuredType.Builder newBuilder(Class<?> implementationClass) {
        return new StructuredType.Builder(implementationClass);
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
                getObjectIdentifier().orElse(null),
                attributes,
                isFinal(),
                isInstantiable,
                comparision,
                superType == null ? null : (StructuredType) superType.copy(),
                getDescription().orElse(null),
                implementationClass);
    }

    @Override
    public String asSummaryString() {
        if (getObjectIdentifier().isPresent()) {
            return asSerializableString();
        }
        assert implementationClass != null;
        // we use *class* to make it visible that this type is unregistered and not confuse it
        // with catalog types
        return "*" + implementationClass.getName() + "*";
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return (implementationClass != null && implementationClass.isAssignableFrom(clazz))
                || INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        StructuredType currentType = this;
        while (currentType != null) {
            if (currentType.implementationClass != null
                    && clazz.isAssignableFrom(currentType.implementationClass)) {
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
        final List<LogicalType> children = new ArrayList<>();
        // add super fields first
        if (superType != null) {
            children.addAll(superType.getChildren());
        }
        // then specific fields
        children.addAll(
                attributes.stream().map(StructuredAttribute::getType).collect(Collectors.toList()));
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
        return isInstantiable == that.isInstantiable
                && attributes.equals(that.attributes)
                && comparision == that.comparision
                && Objects.equals(superType, that.superType)
                && Objects.equals(implementationClass, that.implementationClass);
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
