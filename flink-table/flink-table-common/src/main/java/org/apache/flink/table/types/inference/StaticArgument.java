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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Describes an argument in a static signature that is not overloaded and does not support varargs.
 *
 * <p>Static arguments are a special case of an input type strategy. While built-in functions often
 * require advanced type inference strategies (taking data type families, common type constraints
 * between arguments, customized validation), many functions are fine with a static signature.
 * Static arguments power these basic use cases.
 *
 * <p>Static arguments can take tables, models, or scalar values. Each argument takes a set of
 * {@link StaticArgumentTrait} that enable basic validation by the framework.
 */
@PublicEvolving
public class StaticArgument {

    private static final RowType DUMMY_ROW_TYPE = RowType.of(new NullType());

    private final String name;
    private final @Nullable DataType dataType;
    private final @Nullable Class<?> conversionClass;
    private final boolean isOptional;
    private final EnumSet<StaticArgumentTrait> traits;

    private StaticArgument(
            String name,
            @Nullable DataType dataType,
            @Nullable Class<?> conversionClass,
            boolean isOptional,
            EnumSet<StaticArgumentTrait> traits) {
        this.name = Preconditions.checkNotNull(name, "Name must not be null.");
        this.dataType = dataType;
        this.conversionClass = conversionClass;
        this.isOptional = isOptional;
        this.traits = Preconditions.checkNotNull(traits, "Traits must not be null.");
        checkName();
        checkTraits(traits);
        checkOptionalType();
        checkTableType();
    }

    /**
     * Declares a scalar argument such as {@code f(12)} or {@code f(otherColumn)}.
     *
     * @param name name for the assignment operator e.g. {@code f(myArg => 12)}
     * @param dataType explicit type to which the argument is cast if necessary
     * @param isOptional whether the argument is optional, if optional the corresponding data type
     *     must be nullable
     */
    public static StaticArgument scalar(String name, DataType dataType, boolean isOptional) {
        Preconditions.checkNotNull(dataType, "Data type must not be null.");
        return new StaticArgument(
                name, dataType, null, isOptional, EnumSet.of(StaticArgumentTrait.SCALAR));
    }

    /**
     * Declares a table argument such as {@code f(t => myTable)} or {@code f(t => TABLE myTable))}.
     *
     * <p>The argument can have {@link StaticArgumentTrait#TABLE_AS_ROW} (default) or {@link
     * StaticArgumentTrait#TABLE_AS_SET} semantics.
     *
     * <p>By only providing a conversion class, the argument supports a "polymorphic" behavior. In
     * other words: it accepts tables with an arbitrary number of columns with arbitrary data types.
     * For this case, a class satisfying {@link RowType#supportsOutputConversion(Class)} must be
     * used.
     *
     * @param name name for the assignment operator e.g. {@code f(myArg => 12)}
     * @param conversionClass a class satisfying {@link RowType#supportsOutputConversion(Class)}
     * @param isOptional whether the argument is optional
     * @param traits set of {@link StaticArgumentTrait} requiring {@link StaticArgumentTrait#TABLE}
     */
    public static StaticArgument table(
            String name,
            Class<?> conversionClass,
            boolean isOptional,
            EnumSet<StaticArgumentTrait> traits) {
        Preconditions.checkNotNull(conversionClass, "Conversion class must not be null.");
        final EnumSet<StaticArgumentTrait> enrichedTraits = EnumSet.copyOf(traits);
        enrichedTraits.add(StaticArgumentTrait.TABLE);
        if (!enrichedTraits.contains(StaticArgumentTrait.TABLE_AS_SET)) {
            enrichedTraits.add(StaticArgumentTrait.TABLE_AS_ROW);
        }
        return new StaticArgument(name, null, conversionClass, isOptional, enrichedTraits);
    }

    /**
     * Declares a table argument such as {@code f(t => myTable)} or {@code f(t => TABLE myTable))}.
     *
     * <p>The argument can have {@link StaticArgumentTrait#TABLE_AS_ROW} (default) or {@link
     * StaticArgumentTrait#TABLE_AS_SET} semantics.
     *
     * <p>By providing a concrete data type, the argument only accepts tables with corresponding
     * number of columns and data types. The data type must be a {@link RowType} or {@link
     * StructuredType}.
     *
     * @param name name for the assignment operator e.g. {@code f(myArg => 12)}
     * @param dataType explicit type to which the argument is cast if necessary
     * @param isOptional whether the argument is optional, if optional the corresponding data type
     *     must be nullable
     * @param traits set of {@link StaticArgumentTrait} requiring {@link StaticArgumentTrait#TABLE}
     */
    public static StaticArgument table(
            String name,
            DataType dataType,
            boolean isOptional,
            EnumSet<StaticArgumentTrait> traits) {
        Preconditions.checkNotNull(dataType, "Data type must not be null.");
        return new StaticArgument(name, dataType, null, isOptional, enrichTableTraits(traits));
    }

    private static EnumSet<StaticArgumentTrait> enrichTableTraits(
            EnumSet<StaticArgumentTrait> traits) {
        final EnumSet<StaticArgumentTrait> enrichedTraits = EnumSet.copyOf(traits);
        enrichedTraits.add(StaticArgumentTrait.TABLE);
        if (!enrichedTraits.contains(StaticArgumentTrait.TABLE_AS_SET)) {
            enrichedTraits.add(StaticArgumentTrait.TABLE_AS_ROW);
        }
        return enrichedTraits;
    }

    public String getName() {
        return name;
    }

    public Optional<DataType> getDataType() {
        return Optional.ofNullable(dataType);
    }

    public Optional<Class<?>> getConversionClass() {
        return Optional.ofNullable(conversionClass);
    }

    public boolean isOptional() {
        return isOptional;
    }

    public EnumSet<StaticArgumentTrait> getTraits() {
        return traits;
    }

    public boolean is(StaticArgumentTrait trait) {
        return traits.contains(trait);
    }

    @Override
    public String toString() {
        final StringBuilder s = new StringBuilder();
        // Possible signatures:
        // (myScalar INT)
        // (myTypedTable ROW<i INT> {TABLE BY ROW})
        // (myUntypedTable {TABLE BY ROW})
        s.append(name);
        s.append(" =>");
        if (dataType != null) {
            s.append(" ");
            s.append(dataType);
        }
        if (!traits.equals(EnumSet.of(StaticArgumentTrait.SCALAR))) {
            s.append(" ");
            s.append(
                    traits.stream()
                            .map(Enum::name)
                            .map(n -> n.replace('_', ' '))
                            .collect(Collectors.joining(", ", "{", "}")));
        }
        return s.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StaticArgument that = (StaticArgument) o;
        return isOptional == that.isOptional
                && Objects.equals(name, that.name)
                && Objects.equals(dataType, that.dataType)
                && Objects.equals(conversionClass, that.conversionClass)
                && Objects.equals(traits, that.traits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, conversionClass, isOptional, traits);
    }

    private void checkName() {
        if (!TypeInference.PARAMETER_NAME_FORMAT.test(name)) {
            throw new ValidationException(
                    String.format(
                            "Invalid argument name '%s'. An argument must follow "
                                    + "the pattern [a-zA-Z_$][a-zA-Z_$0-9]*.",
                            name));
        }
    }

    private void checkTraits(EnumSet<StaticArgumentTrait> traits) {
        if (traits.stream().filter(t -> t.getRequirements().isEmpty()).count() != 1) {
            throw new ValidationException(
                    String.format(
                            "Invalid argument traits for argument '%s'. "
                                    + "An argument must be declared as either scalar, table, or model.",
                            name));
        }
        traits.forEach(
                trait ->
                        trait.getRequirements()
                                .forEach(
                                        requirement -> {
                                            if (!traits.contains(requirement)) {
                                                throw new ValidationException(
                                                        String.format(
                                                                "Invalid argument traits for argument '%s'. Trait %s requires %s.",
                                                                name, trait, requirement));
                                            }
                                        }));
    }

    private void checkOptionalType() {
        if (!isOptional) {
            return;
        }
        // e.g. for untyped table arguments
        if (dataType == null) {
            throw new ValidationException("Untyped table arguments must not be optional.");
        }

        final LogicalType type = dataType.getLogicalType();
        if (!type.isNullable() || !type.supportsInputConversion(dataType.getConversionClass())) {
            throw new ValidationException(
                    String.format(
                            "Invalid data type for optional argument '%s'. "
                                    + "An optional argument has to accept null values.",
                            name));
        }
    }

    private void checkTableType() {
        if (!traits.contains(StaticArgumentTrait.TABLE)) {
            return;
        }
        checkPolymorphicTableType();
        checkTypedTableType();
    }

    private void checkPolymorphicTableType() {
        if (dataType != null || conversionClass == null) {
            return;
        }
        if (!DUMMY_ROW_TYPE.supportsInputConversion(conversionClass)) {
            throw new ValidationException(
                    String.format(
                            "Invalid conversion class '%s' for argument '%s'. "
                                    + "Polymorphic, untyped table arguments must use a row class.",
                            conversionClass.getName(), name));
        }
    }

    private void checkTypedTableType() {
        if (dataType == null) {
            return;
        }
        final LogicalType type = dataType.getLogicalType();
        if (traits.contains(StaticArgumentTrait.TABLE)
                && !LogicalTypeChecks.isCompositeType(type)) {
            throw new ValidationException(
                    String.format(
                            "Invalid data type '%s' for table argument '%s'. "
                                    + "Typed table arguments must use a composite type (i.e. row or structured type).",
                            type, name));
        }
        if (is(StaticArgumentTrait.SUPPORT_UPDATES) && !type.is(LogicalTypeRoot.ROW)) {
            throw new ValidationException(
                    String.format(
                            "Invalid data type '%s' for table argument '%s'. "
                                    + "Table arguments that support updates must use a row type.",
                            type, name));
        }
    }
}
