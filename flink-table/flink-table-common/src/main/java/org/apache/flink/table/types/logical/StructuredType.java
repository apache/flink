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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.lang.model.SourceVersion;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.table.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Logical type of a user-defined object structured type. Structured types contain zero, one or more
 * attributes. Each attribute has a name, a type, and an optional description. A type cannot be
 * defined in such a way that one of its attribute types (transitively) refers to itself.
 *
 * <p>Compared to {@link RowType}, which may also be considered a "struct-like" type, structured
 * types are distinguishable even if they contain the same set of fields. For example, "Visit(amount
 * DOUBLE)" is distinct from "Interaction(amount DOUBLE)" due its identifier.
 *
 * <p>There are two kinds of structured types:
 *
 * <h1>Catalog Structured Types</h1>
 *
 * <strong>This type is currently not fully supported in the planner and is future work.</strong>
 *
 * <p>Types that are stored in a catalog and are identified by an {@link ObjectIdentifier}. Some
 * logical properties that align with the SQL standard have been prepared already but are currently
 * not used by the planner:
 *
 * <ul>
 *   <li>super type and single inheritance for more complex type hierarchies, similar to JVM-based
 *       languages.
 *   <li>{@code final} for preventing further inheritance (default behavior) or {@code not final}
 *       for allowing subtypes.
 *   <li>{@code not instantiable} if a more specific type is required or {@code instantiable} if
 *       instances can be created from this type (default behavior).
 *   <li>comparison properties of either {@code none} (no equality), {@code equals} (only equality
 *       and inequality), or {@code full} (greater, equals, less).
 * </ul>
 *
 * <p>NOTE: Compared to the SQL standard, this class is incomplete. We might add new features such
 * as method declarations in the future. Also ordering is not supported yet.
 *
 * <p>The serialized string representation is {@code `cat`.`db`.`t`} where {@code cat} is the
 * catalog name, {@code db} is the database name, and {@code t} the user-defined type name.
 *
 * <h1>Inline Structured Types</h1>
 *
 * <p>Types that are unregistered (i.e. declared inline) and are identified by a class name.
 *
 * <p>The class name does not have to be resolvable in the classpath. It can be used purely to
 * distinguish between two objects containing the same set of attributes. However, in Table API and
 * UDF calls an attempt is being made to resolve the class name to an implementation class. If that
 * fails, {@link Row} is used as the {@link #getDefaultConversion()}.
 *
 * <p>The serialized string representation is {@code STRUCTURED<'c', n0 t0 'd0', n1 t1 'd1', ...>}
 * where {@code c} is the class name, {@code n} is the unique name of a field, {@code t} is the
 * logical type of a field, {@code d} is the optional description of a field.
 *
 * <h1>Implementation Class</h1>
 *
 * <p>A structured type can be defined fully logically. The implementation class is optional and
 * only used at the edges of the table ecosystem (e.g. when bridging to a function or collecting
 * results). Serialization and equality ({@code hashCode/equals}) are handled by the runtime based
 * on the logical type. In other words: {@code hashCode/equals} of an implementation class are not
 * used. Custom equality, casting logic, and further overloaded operators will be supported once we
 * allow defining methods on structured types.
 *
 * <p>An implementation class must offer a default constructor with zero arguments or a full
 * constructor that assigns all attributes. Other physical properties such as the conversion classes
 * of attributes are defined by a {@link DataType} when a structured type is used.
 */
@PublicEvolving
public final class StructuredType extends UserDefinedType {
    private static final long serialVersionUID = 1L;

    public static final String CATALOG_FORMAT = "%s";
    public static final String INLINE_FORMAT = "STRUCTURED<'%s', %s>";

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(Row.class.getName(), RowData.class.getName());

    private static final Class<?> FALLBACK_CONVERSION = Row.class;

    /** Defines an attribute of a {@link StructuredType}. */
    @PublicEvolving
    public static final class StructuredAttribute implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";
        public static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

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

        public String asSummaryString() {
            return formatString(type.asSummaryString(), true);
        }

        public String asSerializableString() {
            return formatString(type.asSerializableString(), false);
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

        private String formatString(String typeString, boolean excludeDescription) {
            if (description == null) {
                return String.format(
                        FIELD_FORMAT_NO_DESCRIPTION, escapeIdentifier(name), typeString);
            } else if (excludeDescription) {
                return String.format(
                        FIELD_FORMAT_WITH_DESCRIPTION, escapeIdentifier(name), typeString, "...");
            } else {
                return String.format(
                        FIELD_FORMAT_WITH_DESCRIPTION,
                        escapeIdentifier(name),
                        typeString,
                        escapeSingleQuotes(description));
            }
        }
    }

    /** Defines equality properties for scalar evaluation. */
    @PublicEvolving
    public enum StructuredComparison {
        EQUALS(true, false),
        FULL(true, true),
        NONE(false, false);
        private final boolean equality;
        private final boolean comparison;

        StructuredComparison(boolean equality, boolean comparison) {
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
    @PublicEvolving
    public static final class Builder {

        private final @Nullable ObjectIdentifier objectIdentifier;
        private final @Nullable String className;
        private final @Nullable Class<?> implementationClass;

        private List<StructuredAttribute> attributes = new ArrayList<>();
        private boolean isNullable = true;
        private boolean isFinal = true;
        private boolean isInstantiable = true;
        private StructuredComparison comparison = StructuredComparison.EQUALS;
        private @Nullable StructuredType superType;
        private @Nullable String description;

        public Builder(String className) {
            this.objectIdentifier = null;
            this.className = Preconditions.checkNotNull(className, "Class name must not be null.");
            this.implementationClass = null;
        }

        public Builder(Class<?> implementationClass) {
            this.objectIdentifier = null;
            this.implementationClass =
                    Preconditions.checkNotNull(
                            implementationClass, "Implementation class must not be null.");
            this.className = implementationClass.getName();
        }

        public Builder(ObjectIdentifier objectIdentifier) {
            this.objectIdentifier =
                    Preconditions.checkNotNull(
                            objectIdentifier, "Object identifier must not be null.");
            this.className = null;
            this.implementationClass = null;
        }

        public Builder(ObjectIdentifier objectIdentifier, Class<?> implementationClass) {
            this.objectIdentifier =
                    Preconditions.checkNotNull(
                            objectIdentifier, "Object identifier must not be null.");
            this.implementationClass =
                    Preconditions.checkNotNull(
                            implementationClass, "Implementation class must not be null.");
            this.className = implementationClass.getName();
        }

        public Builder attributes(List<StructuredAttribute> attributes) {
            this.attributes =
                    List.copyOf(
                            Preconditions.checkNotNull(attributes, "Attributes must not be null."));
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

        public Builder comparison(StructuredComparison comparison) {
            this.comparison =
                    Preconditions.checkNotNull(comparison, "Comparison must not be null.");
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
                    comparison,
                    superType,
                    description,
                    className,
                    implementationClass);
        }
    }

    private final List<StructuredAttribute> attributes;
    private final boolean isInstantiable;
    private final StructuredComparison comparison;
    private final @Nullable StructuredType superType;
    private final @Nullable Class<?> implementationClass;
    private @Nullable String className;

    private StructuredType(
            boolean isNullable,
            ObjectIdentifier objectIdentifier,
            List<StructuredAttribute> attributes,
            boolean isFinal,
            boolean isInstantiable,
            StructuredComparison comparison,
            @Nullable StructuredType superType,
            @Nullable String description,
            @Nullable String className,
            @Nullable Class<?> implementationClass) {
        super(isNullable, LogicalTypeRoot.STRUCTURED_TYPE, objectIdentifier, isFinal, description);

        Preconditions.checkArgument(
                objectIdentifier != null || className != null, "An identifier is missing.");

        this.attributes = attributes;
        this.isInstantiable = isInstantiable;
        this.comparison = comparison;
        this.superType = superType;
        this.className = checkClassName(className);
        this.implementationClass = implementationClass;
    }

    /**
     * Creates a builder for a {@link StructuredType} that is identified by an {@link
     * ObjectIdentifier}.
     */
    public static StructuredType.Builder newBuilder(ObjectIdentifier objectIdentifier) {
        return new StructuredType.Builder(objectIdentifier);
    }

    /**
     * Creates a builder for a {@link StructuredType} that identified by an {@link ObjectIdentifier}
     * but with a resolved implementation class.
     */
    public static StructuredType.Builder newBuilder(
            ObjectIdentifier objectIdentifier, Class<?> implementationClass) {
        return new StructuredType.Builder(objectIdentifier, implementationClass);
    }

    /**
     * Creates a builder for a {@link StructuredType} that is identified by a class name derived
     * from the given implementation class.
     */
    public static StructuredType.Builder newBuilder(Class<?> implementationClass) {
        return new StructuredType.Builder(implementationClass);
    }

    /**
     * Creates a builder for a {@link StructuredType} that is identified by a class name but without
     * a resolved implementation class (i.e. eventually using {@link #FALLBACK_CONVERSION}).
     */
    public static StructuredType.Builder newBuilder(String className) {
        return new StructuredType.Builder(className);
    }

    public List<StructuredAttribute> getAttributes() {
        return attributes;
    }

    public boolean isInstantiable() {
        return isInstantiable;
    }

    public StructuredComparison getComparison() {
        return comparison;
    }

    public Optional<StructuredType> getSuperType() {
        return Optional.ofNullable(superType);
    }

    public Optional<String> getClassName() {
        return Optional.ofNullable(className);
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
                comparison,
                superType == null ? null : (StructuredType) superType.copy(),
                getDescription().orElse(null),
                className,
                implementationClass);
    }

    @Override
    public String asSummaryString() {
        if (getObjectIdentifier().isPresent()) {
            return asSerializableString();
        }
        assert className != null;
        return withNullability(
                INLINE_FORMAT,
                className,
                getAllAttributes().stream()
                        .map(StructuredAttribute::asSummaryString)
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public String asSerializableString() {
        final String identifier =
                getObjectIdentifier().map(ObjectIdentifier::asSerializableString).orElse(null);
        if (identifier != null) {
            return withNullability(CATALOG_FORMAT, identifier);
        }

        assert className != null;
        return withNullability(
                INLINE_FORMAT,
                EncodingUtils.escapeSingleQuotes(className),
                getAllAttributes().stream()
                        .map(StructuredAttribute::asSerializableString)
                        .collect(Collectors.joining(", ")));
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
        return getAllAttributes().stream()
                .map(StructuredAttribute::getType)
                .collect(Collectors.toList());
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
                && comparison == that.comparison
                && Objects.equals(superType, that.superType)
                && Objects.equals(className, that.className);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(), attributes, isInstantiable, comparison, superType, className);
    }

    private Object readResolve() throws ObjectStreamException {
        if (getReference() != null) {
            return this;
        }
        // Before Flink 2.1 the implementation class was used as a reference and equality was not
        // supported. This restores the type with new defaults from old types that potentially
        // landed in savepoints.
        return new StructuredType(
                isNullable(),
                getObjectIdentifier().orElse(null),
                attributes,
                isFinal(),
                isInstantiable,
                StructuredComparison.EQUALS,
                superType,
                getDescription().orElse(null),
                implementationClass != null ? implementationClass.getName() : className,
                implementationClass);
    }

    private List<StructuredAttribute> getAllAttributes() {
        final List<StructuredAttribute> allAttributes = new ArrayList<>();
        // add super fields first
        if (superType != null) {
            allAttributes.addAll(superType.getAllAttributes());
        }
        // then specific fields
        allAttributes.addAll(attributes);
        return allAttributes;
    }

    /** Unified method for referring to both catalog or inline structured types. */
    private Object getReference() {
        return getObjectIdentifier().map(Object.class::cast).orElse(className);
    }

    private static @Nullable String checkClassName(@Nullable String className) {
        if (className != null && !SourceVersion.isName(className)) {
            throw new ValidationException(
                    String.format(
                            "Invalid class name '%s'. The class name must comply with JVM identifier rules.",
                            className));
        }
        return className;
    }

    // --------------------------------------------------------------------------------------------
    // Shared helpers
    // --------------------------------------------------------------------------------------------

    /**
     * Restores an implementation class from the class name component of a serialized string
     * representation.
     *
     * <p>Note: This method does not perform any kind of validation. The logical type system should
     * not be destabilized by incorrectly implemented classes. This is also why classes won't get
     * initialized. At this stage, only the class existence (i.e. metadata) in classloader matters.
     */
    public static Optional<Class<?>> resolveClass(ClassLoader classLoader, String className) {
        checkClassName(className);
        try {
            // Initialization is deferred until first instantiation
            return Optional.of(Class.forName(className, false, classLoader));
        } catch (Throwable t) {
            return Optional.empty();
        }
    }
}
