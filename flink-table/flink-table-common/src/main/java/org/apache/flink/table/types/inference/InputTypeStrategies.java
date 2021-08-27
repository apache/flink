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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.AndArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.AnyArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.CommonArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.CommonInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ComparableTypeStrategy;
import org.apache.flink.table.types.inference.strategies.CompositeArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ConstraintArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ExplicitArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.FamilyArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.LiteralArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OrArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OrInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OutputArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.RootArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.SequenceInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.SubsequenceInputTypeStrategy.SubsequenceStrategyBuilder;
import org.apache.flink.table.types.inference.strategies.SymbolArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.TypeLiteralArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.VaryingSequenceInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.WildcardInputTypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Strategies for inferring and validating input arguments in a function call.
 *
 * @see InputTypeStrategy
 * @see ArgumentTypeStrategy
 */
@Internal
public final class InputTypeStrategies {

    // --------------------------------------------------------------------------------------------
    // Input type strategies
    // --------------------------------------------------------------------------------------------

    /** Strategy that does not perform any modification or validation of the input. */
    public static final WildcardInputTypeStrategy WILDCARD = new WildcardInputTypeStrategy();

    /** Strategy that does not expect any arguments. */
    public static final InputTypeStrategy NO_ARGS = sequence();

    /**
     * Strategy for a function signature like {@code f(STRING, NUMERIC)} using a sequence of {@link
     * ArgumentTypeStrategy}s.
     */
    public static InputTypeStrategy sequence(ArgumentTypeStrategy... strategies) {
        return new SequenceInputTypeStrategy(Arrays.asList(strategies), null);
    }

    /**
     * Strategy for a named function signature like {@code f(s STRING, n NUMERIC)} using a sequence
     * of {@link ArgumentTypeStrategy}s.
     */
    public static InputTypeStrategy sequence(
            String[] argumentNames, ArgumentTypeStrategy[] strategies) {
        return new SequenceInputTypeStrategy(
                Arrays.asList(strategies), Arrays.asList(argumentNames));
    }

    /**
     * Strategy for a varying function signature like {@code f(INT, STRING, NUMERIC...)} using a
     * sequence of {@link ArgumentTypeStrategy}s. The first n - 1 arguments must be constant. The
     * n-th argument can occur 0, 1, or more times.
     */
    public static InputTypeStrategy varyingSequence(ArgumentTypeStrategy... strategies) {
        return new VaryingSequenceInputTypeStrategy(Arrays.asList(strategies), null);
    }

    /**
     * Strategy for a varying named function signature like {@code f(i INT, str STRING, num
     * NUMERIC...)} using a sequence of {@link ArgumentTypeStrategy}s. The first n - 1 arguments
     * must be constant. The n-th argument can occur 0, 1, or more times.
     */
    public static InputTypeStrategy varyingSequence(
            String[] argumentNames, ArgumentTypeStrategy[] strategies) {
        return new VaryingSequenceInputTypeStrategy(
                Arrays.asList(strategies), Arrays.asList(argumentNames));
    }

    /**
     * Strategy for a function signature of explicitly defined types like {@code f(STRING, INT)}.
     * Implicit casts will be inserted if possible.
     *
     * <p>This is equivalent to using {@link #sequence(ArgumentTypeStrategy...)} and {@link
     * #explicit(DataType)}.
     */
    public static InputTypeStrategy explicitSequence(DataType... expectedDataTypes) {
        final List<ArgumentTypeStrategy> strategies =
                Arrays.stream(expectedDataTypes)
                        .map(InputTypeStrategies::explicit)
                        .collect(Collectors.toList());
        return new SequenceInputTypeStrategy(strategies, null);
    }

    /**
     * Strategy for a named function signature of explicitly defined types like {@code f(s STRING, i
     * INT)}. Implicit casts will be inserted if possible.
     *
     * <p>This is equivalent to using {@link #sequence(String[], ArgumentTypeStrategy[])} and {@link
     * #explicit(DataType)}.
     */
    public static InputTypeStrategy explicitSequence(
            String[] argumentNames, DataType[] expectedDataTypes) {
        final List<ArgumentTypeStrategy> strategies =
                Arrays.stream(expectedDataTypes)
                        .map(InputTypeStrategies::explicit)
                        .collect(Collectors.toList());
        return new SequenceInputTypeStrategy(strategies, Arrays.asList(argumentNames));
    }

    /**
     * An strategy that lets you apply other strategies for subsequences of the actual arguments.
     *
     * <p>The {@link #sequence(ArgumentTypeStrategy...)} should be preferred in most of the cases.
     * Use this strategy only if you need to apply a common logic to a subsequence of the arguments.
     */
    public static SubsequenceStrategyBuilder compositeSequence() {
        return new SubsequenceStrategyBuilder();
    }

    /**
     * Strategy for a disjunction of multiple {@link InputTypeStrategy}s into one like {@code
     * f(NUMERIC) || f(STRING)}.
     *
     * <p>This strategy aims to infer a list of types that are equal to the input types (to prevent
     * unnecessary casting) or (if this is not possible) the first more specific, casted types.
     */
    public static InputTypeStrategy or(InputTypeStrategy... strategies) {
        return new OrInputTypeStrategy(Arrays.asList(strategies));
    }

    /**
     * Strategy that does not perform any modification or validation of the input. It checks the
     * argument count though.
     */
    public static InputTypeStrategy wildcardWithCount(ArgumentCount argumentCount) {
        return new WildcardInputTypeStrategy(argumentCount);
    }

    /**
     * Strategy that checks all types are comparable with each other. Requires at least one
     * argument.
     */
    public static InputTypeStrategy comparable(
            ConstantArgumentCount argumentCount, StructuredComparison requiredComparison) {
        return new ComparableTypeStrategy(argumentCount, requiredComparison);
    }

    // --------------------------------------------------------------------------------------------
    // Argument type strategies
    // --------------------------------------------------------------------------------------------

    /**
     * Strategy for inferring an unknown argument type from the function's output {@link DataType}
     * if available.
     */
    public static final OutputArgumentTypeStrategy OUTPUT_IF_NULL =
            new OutputArgumentTypeStrategy();

    /** Strategy for an argument that can be of any type. */
    public static final AnyArgumentTypeStrategy ANY = new AnyArgumentTypeStrategy();

    /** Strategy that checks if an argument is a literal. */
    public static final LiteralArgumentTypeStrategy LITERAL =
            new LiteralArgumentTypeStrategy(false);

    /** Strategy that checks if an argument is a literal or NULL. */
    public static final LiteralArgumentTypeStrategy LITERAL_OR_NULL =
            new LiteralArgumentTypeStrategy(true);

    /** Strategy that checks if an argument is a type literal. */
    public static final TypeLiteralArgumentTypeStrategy TYPE_LITERAL =
            new TypeLiteralArgumentTypeStrategy();

    /** Strategy that checks that the argument has a composite type. */
    public static final ArgumentTypeStrategy COMPOSITE = new CompositeArgumentTypeStrategy();

    /**
     * Argument type strategy that checks and casts for a common, least restrictive type of all
     * arguments.
     */
    public static final ArgumentTypeStrategy COMMON_ARG = new CommonArgumentTypeStrategy(false);

    /**
     * Argument type strategy that checks and casts for a common, least restrictive type of all
     * arguments. But leaves nullability untouched.
     */
    public static final ArgumentTypeStrategy COMMON_ARG_NULLABLE =
            new CommonArgumentTypeStrategy(true);

    /**
     * Strategy for an argument that corresponds to an explicitly defined type casting. Implicit
     * casts will be inserted if possible.
     */
    public static ExplicitArgumentTypeStrategy explicit(DataType expectedDataType) {
        return new ExplicitArgumentTypeStrategy(expectedDataType);
    }

    /**
     * Strategy for an argument that corresponds to a given {@link LogicalTypeRoot}. Implicit casts
     * will be inserted if possible.
     */
    public static RootArgumentTypeStrategy logical(LogicalTypeRoot expectedRoot) {
        return new RootArgumentTypeStrategy(expectedRoot, null);
    }

    /**
     * Strategy for an argument that corresponds to a given {@link LogicalTypeRoot} and nullability.
     * Implicit casts will be inserted if possible.
     */
    public static RootArgumentTypeStrategy logical(
            LogicalTypeRoot expectedRoot, boolean expectedNullability) {
        return new RootArgumentTypeStrategy(expectedRoot, expectedNullability);
    }

    /**
     * Strategy for an argument that corresponds to a given {@link LogicalTypeFamily}. Implicit
     * casts will be inserted if possible.
     */
    public static FamilyArgumentTypeStrategy logical(LogicalTypeFamily expectedFamily) {
        return new FamilyArgumentTypeStrategy(expectedFamily, null);
    }

    /**
     * Strategy for an argument that corresponds to a given {@link LogicalTypeFamily} and
     * nullability. Implicit casts will be inserted if possible.
     */
    public static FamilyArgumentTypeStrategy logical(
            LogicalTypeFamily expectedFamily, boolean expectedNullability) {
        return new FamilyArgumentTypeStrategy(expectedFamily, expectedNullability);
    }

    /** Strategy for an argument that must fulfill a given constraint. */
    public static ConstraintArgumentTypeStrategy constraint(
            String constraintMessage, Function<List<DataType>, Boolean> evaluator) {
        return new ConstraintArgumentTypeStrategy(constraintMessage, evaluator);
    }

    /**
     * Strategy for a conjunction of multiple {@link ArgumentTypeStrategy}s into one like {@code
     * f(NUMERIC && LITERAL)}.
     *
     * <p>Some {@link ArgumentTypeStrategy}s cannot contribute an inferred type that is different
     * from the input type (e.g. {@link #LITERAL}). Therefore, the order {@code f(X && Y)} or {@code
     * f(Y && X)} matters as it defines the precedence in case the result must be casted to a more
     * specific type.
     *
     * <p>This strategy aims to infer the first more specific, casted type or (if this is not
     * possible) a type that has been inferred from all {@link ArgumentTypeStrategy}s.
     */
    public static AndArgumentTypeStrategy and(ArgumentTypeStrategy... strategies) {
        return new AndArgumentTypeStrategy(Arrays.asList(strategies));
    }

    /**
     * Strategy for a disjunction of multiple {@link ArgumentTypeStrategy}s into one like {@code
     * f(NUMERIC || STRING)}.
     *
     * <p>Some {@link ArgumentTypeStrategy}s cannot contribute an inferred type that is different
     * from the input type (e.g. {@link #LITERAL}). Therefore, the order {@code f(X || Y)} or {@code
     * f(Y || X)} matters as it defines the precedence in case the result must be casted to a more
     * specific type.
     *
     * <p>This strategy aims to infer a type that is equal to the input type (to prevent unnecessary
     * casting) or (if this is not possible) the first more specific, casted type.
     */
    public static OrArgumentTypeStrategy or(ArgumentTypeStrategy... strategies) {
        return new OrArgumentTypeStrategy(Arrays.asList(strategies));
    }

    /** Strategy for a symbol argument of a specific {@link TableSymbol} enum. */
    public static SymbolArgumentTypeStrategy symbol(
            Class<? extends Enum<? extends TableSymbol>> clazz) {
        return new SymbolArgumentTypeStrategy(clazz);
    }

    /**
     * An {@link InputTypeStrategy} that expects {@code count} arguments that have a common type.
     */
    public static InputTypeStrategy commonType(int count) {
        return new CommonInputTypeStrategy(ConstantArgumentCount.of(count));
    }

    // --------------------------------------------------------------------------------------------

    private InputTypeStrategies() {
        // no instantiation
    }
}
