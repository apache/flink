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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Optional;

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
        StaticArgumentTrait.checkIntegrity(
                Preconditions.checkNotNull(traits, "Traits must not be null."));
        this.name = Preconditions.checkNotNull(name, "Name must not be null.");
        this.dataType = dataType;
        this.conversionClass = conversionClass;
        this.isOptional = isOptional;
        this.traits = traits;
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
}
