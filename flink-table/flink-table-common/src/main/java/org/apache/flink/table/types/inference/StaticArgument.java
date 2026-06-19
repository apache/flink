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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final List<ConditionalTrait> conditionalTraits;

    private StaticArgument(
            String name,
            @Nullable DataType dataType,
            @Nullable Class<?> conversionClass,
            boolean isOptional,
            EnumSet<StaticArgumentTrait> traits) {
        this(name, dataType, conversionClass, isOptional, traits, List.of());
    }

    private StaticArgument(
            String name,
            @Nullable DataType dataType,
            @Nullable Class<?> conversionClass,
            boolean isOptional,
            EnumSet<StaticArgumentTrait> traits,
            List<ConditionalTrait> conditionalTraits) {
        this.name = Preconditions.checkNotNull(name, "Name must not be null.");
        this.dataType = dataType;
        this.conversionClass = conversionClass;
        this.isOptional = isOptional;
        this.traits = Preconditions.checkNotNull(traits, "Traits must not be null.");
        this.conditionalTraits = conditionalTraits;
        checkName();
        checkTraits(traits);
        checkOptionalType();
        checkTableType();
        checkModelType();
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
     * <p>The argument can have {@link StaticArgumentTrait#ROW_SEMANTIC_TABLE} (default) or {@link
     * StaticArgumentTrait#SET_SEMANTIC_TABLE} semantics.
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
        if (!enrichedTraits.contains(StaticArgumentTrait.SET_SEMANTIC_TABLE)) {
            enrichedTraits.add(StaticArgumentTrait.ROW_SEMANTIC_TABLE);
        }
        return new StaticArgument(name, null, conversionClass, isOptional, enrichedTraits);
    }

    /**
     * Declares a table argument such as {@code f(t => myTable)} or {@code f(t => TABLE myTable))}.
     *
     * <p>The argument can have {@link StaticArgumentTrait#ROW_SEMANTIC_TABLE} (default) or {@link
     * StaticArgumentTrait#SET_SEMANTIC_TABLE} semantics.
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

    /**
     * Declares a model argument such as {@code f(m => myModel)} or {@code f(m => MODEL myModel))}.
     *
     * <p>By using this method, the argument supports a "polymorphic" behavior. In other words: it
     * accepts models with arbitrary schemas or types.
     *
     * @param name name for the assignment operator e.g. {@code f(myArg => myModel)}
     * @param isOptional whether the argument is optional
     * @param traits set of {@link StaticArgumentTrait} requiring {@link StaticArgumentTrait#MODEL}
     */
    public static StaticArgument model(
            String name, boolean isOptional, EnumSet<StaticArgumentTrait> traits) {
        final EnumSet<StaticArgumentTrait> enrichedTraits = EnumSet.copyOf(traits);
        enrichedTraits.add(StaticArgumentTrait.MODEL);
        return new StaticArgument(name, null, null, isOptional, enrichedTraits);
    }

    private static EnumSet<StaticArgumentTrait> enrichTableTraits(
            EnumSet<StaticArgumentTrait> traits) {
        final EnumSet<StaticArgumentTrait> enrichedTraits = EnumSet.copyOf(traits);
        enrichedTraits.add(StaticArgumentTrait.TABLE);
        if (!enrichedTraits.contains(StaticArgumentTrait.SET_SEMANTIC_TABLE)) {
            enrichedTraits.add(StaticArgumentTrait.ROW_SEMANTIC_TABLE);
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

    /**
     * Context-aware trait check. Evaluates conditional trait rules against the given context to
     * determine the effective traits.
     */
    public boolean is(StaticArgumentTrait trait, TraitContext ctx) {
        return resolveTraits(ctx).contains(trait);
    }

    /**
     * Returns a new {@link StaticArgument} with an additional conditional trait rule. The trait is
     * added to the effective trait set when the condition evaluates to {@code true} at planning
     * time. Only non-root traits (subtraits of TABLE, SCALAR, or MODEL) are allowed.
     *
     * <p>Multiple conditions for the same trait use OR semantics: the trait is activated if any of
     * its conditions is met.
     *
     * <p>Example:
     *
     * <pre>{@code
     * StaticArgument.table("input", Row.class, false, EnumSet.of(TABLE, SUPPORT_UPDATES))
     *         .withConditionalTrait(SET_SEMANTIC_TABLE, hasPartitionBy());
     * }</pre>
     */
    public StaticArgument withConditionalTrait(
            final StaticArgumentTrait trait, final TraitCondition condition) {
        if (trait.isRoot()) {
            throw new IllegalArgumentException(
                    "Root traits (SCALAR, TABLE, MODEL) cannot be conditional.");
        }
        final List<ConditionalTrait> accumulated = new ArrayList<>(this.conditionalTraits);
        accumulated.add(new ConditionalTrait(condition, trait));
        return new StaticArgument(name, dataType, conversionClass, isOptional, traits, accumulated);
    }

    /** Whether this argument has conditional trait rules. */
    public boolean hasConditionalTraits() {
        return !conditionalTraits.isEmpty();
    }

    /** Whether any conditional trait rule may add the given trait. */
    public boolean hasConditionalTrait(final StaticArgumentTrait trait) {
        return conditionalTraits.stream().anyMatch(c -> c.trait == trait);
    }

    /**
     * Returns a new {@link StaticArgument} with conditional traits resolved against the given
     * context. The returned argument has the effective traits baked in and no conditional rules.
     */
    public StaticArgument applyConditionalTraits(final TraitContext ctx) {
        if (conditionalTraits.isEmpty()) {
            return this;
        }
        return new StaticArgument(name, dataType, conversionClass, isOptional, resolveTraits(ctx));
    }

    /**
     * Resolves effective traits by evaluating conditional rules against the context. Returns the
     * base traits combined with any conditional traits whose conditions are met.
     */
    public EnumSet<StaticArgumentTrait> resolveTraits(final TraitContext ctx) {
        if (conditionalTraits.isEmpty()) {
            return traits;
        }
        final EnumSet<StaticArgumentTrait> resolved = EnumSet.copyOf(traits);
        for (final ConditionalTrait conditionalTrait : conditionalTraits) {
            if (conditionalTrait.condition.test(ctx)) {
                removeMutuallyExclusiveTraits(resolved, conditionalTrait.trait);
                resolved.add(conditionalTrait.trait);
            }
        }
        return resolved;
    }

    private static void removeMutuallyExclusiveTraits(
            final EnumSet<StaticArgumentTrait> traits, final StaticArgumentTrait adding) {
        traits.removeAll(adding.getIncompatibleWith());
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
            final Stream<String> baseTraitNames =
                    traits.stream().map(Enum::name).map(n -> n.replace('_', ' '));
            final Stream<String> conditionalTraitNames =
                    conditionalTraits.stream().map(c -> c.trait.name().replace('_', ' '));
            s.append(" ");
            s.append(
                    Stream.concat(baseTraitNames, conditionalTraitNames)
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
                && Objects.equals(traits, that.traits)
                && Objects.equals(conditionalTraits, that.conditionalTraits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, conversionClass, isOptional, traits, conditionalTraits);
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
            return;
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
        checkTableNotOptional();
        checkPolymorphicTableType();
        checkTypedTableType();
    }

    private void checkModelType() {
        if (!traits.contains(StaticArgumentTrait.MODEL)) {
            return;
        }
        checkModelNotOptional();
    }

    private void checkTableNotOptional() {
        if (isOptional) {
            throw new ValidationException("Table arguments must not be optional.");
        }
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

    private void checkModelNotOptional() {
        if (isOptional) {
            throw new ValidationException("Model arguments must not be optional.");
        }
    }

    /** A trait that is conditionally added based on a {@link TraitCondition}. */
    private static final class ConditionalTrait {
        private final TraitCondition condition;
        private final StaticArgumentTrait trait;

        ConditionalTrait(final TraitCondition condition, final StaticArgumentTrait trait) {
            this.condition = condition;
            this.trait = trait;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ConditionalTrait that = (ConditionalTrait) o;
            return Objects.equals(condition, that.condition) && trait == that.trait;
        }

        @Override
        public int hashCode() {
            return Objects.hash(condition, trait);
        }
    }
}
