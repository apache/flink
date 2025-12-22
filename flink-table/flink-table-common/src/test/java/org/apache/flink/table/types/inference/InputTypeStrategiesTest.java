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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.utils.TypeConversions;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.NULL;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL_OR_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.OUTPUT_IF_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.WILDCARD;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.arrayOf;
import static org.apache.flink.table.types.inference.InputTypeStrategies.constraint;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicitSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.INDEX;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.percentage;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.percentageArray;

/** Tests for built-in {@link InputTypeStrategies}. */
class InputTypeStrategiesTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(defaultTestCases(), arrayOfArgumentTestCases()).flatMap(s -> s);
    }

    private Stream<TestSpec> defaultTestCases() {
        return Stream.of(
                // wildcard with 2 arguments
                TestSpec.forStrategy(WILDCARD)
                        .calledWithArgumentTypes(INT(), INT())
                        .expectSignature("f(*)")
                        .expectArgumentTypes(INT(), INT()),

                // wildcard with 0 arguments
                TestSpec.forStrategy(WILDCARD)
                        .calledWithArgumentTypes()
                        .expectSignature("f(*)")
                        .expectArgumentTypes(),

                // explicit sequence
                TestSpec.forStrategy(explicitSequence(INT().bridgedTo(int.class), BOOLEAN()))
                        .calledWithArgumentTypes(INT(), BOOLEAN())
                        .expectSignature("f(INT, BOOLEAN)")
                        .expectArgumentTypes(INT().bridgedTo(int.class), BOOLEAN()),

                // explicit sequence with ROW ignoring field names
                TestSpec.forStrategy(explicitSequence(ROW(FIELD("expected", INT()))))
                        .calledWithArgumentTypes(ROW(FIELD("actual", INT())))
                        .expectSignature("f(ROW<`expected` INT>)")
                        .expectArgumentTypes(ROW(FIELD("expected", INT()))),

                // invalid named sequence
                TestSpec.forStrategy(
                                explicitSequence(
                                        new String[] {"i", "s"}, new DataType[] {INT(), STRING()}))
                        .calledWithArgumentTypes(INT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(i INT, s STRING)"),

                // incompatible nullability
                TestSpec.forStrategy(explicitSequence(BIGINT().notNull()))
                        .calledWithArgumentTypes(BIGINT())
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type 'BIGINT NOT NULL' but actual type was 'BIGINT'."),

                // implicit cast
                TestSpec.forStrategy(explicitSequence(BIGINT()))
                        .calledWithArgumentTypes(INT())
                        .expectArgumentTypes(BIGINT()),

                // incompatible types
                TestSpec.forStrategy(explicitSequence(BIGINT()))
                        .calledWithArgumentTypes(STRING())
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type 'BIGINT' but actual type was 'STRING'."),

                // incompatible number of arguments
                TestSpec.forStrategy(explicitSequence(BIGINT(), BIGINT()))
                        .calledWithArgumentTypes(BIGINT())
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 2 arguments expected but 1 passed."),

                // any type
                TestSpec.forStrategy(sequence(ANY))
                        .calledWithArgumentTypes(BIGINT())
                        .expectSignature("f(<ANY>)")
                        .expectArgumentTypes(BIGINT()),

                // incompatible number of arguments
                TestSpec.forStrategy(sequence(ANY))
                        .calledWithArgumentTypes(BIGINT(), BIGINT())
                        .expectErrorMessage(
                                "Invalid number of arguments. At most 1 arguments expected but 2 passed."),
                TestSpec.forStrategy(
                                "OR with bridging class",
                                or(
                                        explicitSequence(STRING()),
                                        explicitSequence(INT().bridgedTo(int.class)),
                                        explicitSequence(BOOLEAN())))
                        .calledWithArgumentTypes(INT())
                        .calledWithArgumentTypes(TINYINT())
                        .expectSignature("f(STRING)\nf(INT)\nf(BOOLEAN)")
                        .expectArgumentTypes(INT().bridgedTo(int.class)),
                TestSpec.forStrategy(
                                "OR with implicit casting",
                                or(
                                        explicitSequence(TINYINT()),
                                        explicitSequence(INT()),
                                        explicitSequence(BIGINT())))
                        .calledWithArgumentTypes(SMALLINT())
                        .expectArgumentTypes(INT()),
                TestSpec.forStrategy(
                                "OR with implicit casting of null",
                                or(
                                        explicitSequence(STRING().notNull()),
                                        explicitSequence(INT().notNull()),
                                        explicitSequence(BIGINT())))
                        .calledWithArgumentTypes(NULL())
                        .expectArgumentTypes(BIGINT()),
                TestSpec.forStrategy(
                                "OR with implicit casting using first match",
                                or(explicitSequence(VARCHAR(20)), explicitSequence(VARCHAR(10))))
                        .calledWithArgumentTypes(VARCHAR(1))
                        .expectArgumentTypes(VARCHAR(20)),
                TestSpec.forStrategy(
                                "OR with invalid implicit casting of null",
                                or(
                                        explicitSequence(STRING().notNull()),
                                        explicitSequence(INT().notNull()),
                                        explicitSequence(BIGINT().notNull())))
                        .calledWithArgumentTypes(NULL())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "f(STRING NOT NULL)\nf(INT NOT NULL)\nf(BIGINT NOT NULL)"),
                TestSpec.forStrategy(
                                "OR with invalid type",
                                or(explicitSequence(INT()), explicitSequence(STRING())))
                        .calledWithArgumentTypes(BOOLEAN())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(INT)\nf(STRING)"),

                // invalid typed sequence
                TestSpec.forStrategy(explicitSequence(INT(), BOOLEAN()))
                        .calledWithArgumentTypes(BOOLEAN(), INT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(INT, BOOLEAN)"),

                // sequence with wildcard
                TestSpec.forStrategy(sequence(ANY, explicit(INT())))
                        .calledWithArgumentTypes(BOOLEAN(), INT())
                        .calledWithArgumentTypes(BOOLEAN(), TINYINT())
                        .expectArgumentTypes(BOOLEAN(), INT()),

                // invalid named sequence
                TestSpec.forStrategy(
                                sequence(
                                        new String[] {"any", "int"},
                                        new ArgumentTypeStrategy[] {ANY, explicit(INT())}))
                        .calledWithArgumentTypes(STRING(), BOOLEAN())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(any <ANY>, int INT)"),

                // sequence with OR and implicit casting
                TestSpec.forStrategy(
                                sequence(explicit(INT()), or(explicit(BOOLEAN()), explicit(INT()))))
                        .expectSignature("f(INT, [BOOLEAN | INT])")
                        .calledWithArgumentTypes(INT(), INT())
                        .calledWithArgumentTypes(TINYINT(), TINYINT())
                        .expectArgumentTypes(INT(), INT()),

                // sequence with OR
                TestSpec.forStrategy(
                                sequence(
                                        explicit(INT()),
                                        or(explicit(BOOLEAN()), explicit(STRING()))))
                        .calledWithArgumentTypes(INT(), BIGINT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(INT, [BOOLEAN | STRING])"),

                // sequence with literal
                TestSpec.forStrategy(sequence(LITERAL))
                        .calledWithLiteralAt(0)
                        .calledWithArgumentTypes(INT())
                        .expectArgumentTypes(INT()),

                // sequence with literal
                TestSpec.forStrategy(sequence(and(LITERAL, explicit(STRING())), explicit(INT())))
                        .calledWithLiteralAt(0)
                        .calledWithArgumentTypes(STRING(), INT())
                        .expectSignature("f([<LITERAL NOT NULL> & STRING], INT)")
                        .expectArgumentTypes(STRING(), INT()),

                // sequence with missing literal
                TestSpec.forStrategy(
                                sequence(and(explicit(STRING()), LITERAL_OR_NULL), explicit(INT())))
                        .calledWithArgumentTypes(STRING(), INT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf([STRING & <LITERAL>], INT)"),

                // vararg sequence
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"i", "s", "var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(INT()), explicit(STRING()), explicit(BOOLEAN())
                                        }))
                        .calledWithArgumentTypes(INT(), STRING(), BOOLEAN(), BOOLEAN(), BOOLEAN())
                        .expectArgumentTypes(INT(), STRING(), BOOLEAN(), BOOLEAN(), BOOLEAN()),

                // vararg sequence with conversion class
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(BOOLEAN().bridgedTo(boolean.class))
                                        }))
                        .calledWithArgumentTypes(BOOLEAN(), BOOLEAN(), BOOLEAN())
                        .expectSignature("f(var BOOLEAN...)")
                        .expectArgumentTypes(
                                BOOLEAN().bridgedTo(boolean.class),
                                BOOLEAN().bridgedTo(boolean.class),
                                BOOLEAN().bridgedTo(boolean.class)),

                // vararg sequence
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"i", "s", "var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(INT()), explicit(STRING()), explicit(BOOLEAN())
                                        }))
                        .calledWithArgumentTypes(INT(), STRING())
                        .expectArgumentTypes(INT(), STRING()),

                // invalid vararg type
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"i", "s", "var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(INT()), explicit(STRING()), explicit(BOOLEAN())
                                        }))
                        .calledWithArgumentTypes(INT(), STRING(), STRING())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var BOOLEAN...)"),

                // invalid non-vararg type
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"i", "s", "var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(INT()), explicit(STRING()), explicit(BOOLEAN())
                                        }))
                        .calledWithArgumentTypes(INT(), INT(), BOOLEAN())
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type 'STRING' but actual type was 'INT'."),

                // OR in vararg type
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"i", "s", "var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(INT()),
                                            explicit(STRING()),
                                            or(explicit(BOOLEAN()), explicit(INT()))
                                        }))
                        .calledWithArgumentTypes(INT(), STRING(), INT(), BOOLEAN())
                        .expectArgumentTypes(INT(), STRING(), INT(), BOOLEAN()),

                // invalid OR in vararg type
                TestSpec.forStrategy(
                                varyingSequence(
                                        new String[] {"i", "s", "var"},
                                        new ArgumentTypeStrategy[] {
                                            explicit(INT()),
                                            explicit(STRING()),
                                            or(explicit(BOOLEAN()), explicit(INT()))
                                        }))
                        .calledWithArgumentTypes(INT(), STRING(), STRING(), STRING())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var [BOOLEAN | INT]...)"),

                // incomplete inference
                TestSpec.forStrategy(WILDCARD)
                        .calledWithArgumentTypes(NULL(), STRING(), NULL())
                        .expectSignature("f(*)")
                        .expectArgumentTypes(NULL(), STRING(), NULL()),

                // typed arguments help inferring a type
                TestSpec.forStrategy(WILDCARD)
                        .typedArguments(INT().bridgedTo(int.class), STRING(), BOOLEAN())
                        .calledWithArgumentTypes(NULL(), STRING(), NULL())
                        .expectArgumentTypes(INT().bridgedTo(int.class), STRING(), BOOLEAN()),

                // surrounding function helps inferring a type
                TestSpec.forStrategy(sequence(OUTPUT_IF_NULL, OUTPUT_IF_NULL, OUTPUT_IF_NULL))
                        .surroundingStrategy(explicitSequence(BOOLEAN()))
                        .calledWithArgumentTypes(NULL(), STRING(), NULL())
                        .expectSignature("f(<OUTPUT>, <OUTPUT>, <OUTPUT>)")
                        .expectArgumentTypes(BOOLEAN(), STRING(), BOOLEAN()),

                // surrounding function helps inferring a type
                TestSpec.forStrategy(sequence(or(OUTPUT_IF_NULL, explicit(INT()))))
                        .surroundingStrategy(explicitSequence(BOOLEAN()))
                        .calledWithArgumentTypes(NULL())
                        .expectSignature("f([<OUTPUT> | INT])")
                        .expectArgumentTypes(BOOLEAN()),

                // surrounding info can not infer input type and does not help inferring a type
                TestSpec.forStrategy(explicitSequence(BOOLEAN()))
                        .surroundingStrategy(WILDCARD)
                        .calledWithArgumentTypes(NULL())
                        .expectSignature("f(BOOLEAN)")
                        .expectArgumentTypes(BOOLEAN()),

                // surrounding function does not help inferring a type
                TestSpec.forStrategy(sequence(or(OUTPUT_IF_NULL, explicit(INT()))))
                        .calledWithArgumentTypes(NULL())
                        .expectSignature("f([<OUTPUT> | INT])")
                        .expectArgumentTypes(INT()),

                // typed arguments only with casting
                TestSpec.forStrategy(WILDCARD)
                        .typedArguments(INT(), STRING())
                        .calledWithArgumentTypes(TINYINT(), STRING())
                        .expectSignature("f(arg0 => INT, arg1 => STRING)")
                        .expectArgumentTypes(INT(), STRING()),

                // invalid typed arguments
                TestSpec.forStrategy(WILDCARD)
                        .typedArguments(INT(), STRING())
                        .calledWithArgumentTypes(STRING(), STRING())
                        .expectErrorMessage(
                                "Invalid argument type at position 0. Data type INT expected but STRING passed."),

                // named arguments
                TestSpec.forStrategy(WILDCARD)
                        .namedArguments("i", "s")
                        .typedArguments(INT(), STRING())
                        .expectSignature("f(i => INT, s => STRING)"),
                TestSpec.forStrategy(
                                "Wildcard with count verifies arguments number",
                                InputTypeStrategies.wildcardWithCount(
                                        ConstantArgumentCount.from(2)))
                        .calledWithArgumentTypes(STRING())
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 2 arguments expected but 1 passed."),
                TestSpec.forStrategy(
                                "Array strategy infers a common type",
                                SpecificInputTypeStrategies.ARRAY)
                        .expectSignature("f(<COMMON>, <COMMON>...)")
                        .calledWithArgumentTypes(
                                INT().notNull(), BIGINT().notNull(), DOUBLE(), DOUBLE().notNull())
                        .expectArgumentTypes(DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()),
                TestSpec.forStrategy(
                                "Array strategy fails for no arguments",
                                SpecificInputTypeStrategies.ARRAY)
                        .calledWithArgumentTypes()
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 1 arguments expected but 0 passed."),
                TestSpec.forStrategy(
                                "Array strategy fails for null arguments",
                                SpecificInputTypeStrategies.ARRAY)
                        .calledWithArgumentTypes(NULL())
                        .expectErrorMessage("Could not find a common type for arguments: [NULL]"),
                TestSpec.forStrategy(
                                "Map strategy infers common types", SpecificInputTypeStrategies.MAP)
                        .calledWithArgumentTypes(
                                INT().notNull(), DOUBLE(), BIGINT().notNull(), FLOAT().notNull())
                        .expectArgumentTypes(
                                BIGINT().notNull(), DOUBLE(), BIGINT().notNull(), DOUBLE()),
                TestSpec.forStrategy(
                                "Map strategy fails for no arguments",
                                SpecificInputTypeStrategies.MAP)
                        .calledWithArgumentTypes()
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 2 arguments expected but 0 passed."),
                TestSpec.forStrategy(
                                "Map strategy fails for an odd number of arguments",
                                SpecificInputTypeStrategies.MAP)
                        .calledWithArgumentTypes(BIGINT(), BIGINT(), BIGINT())
                        .expectErrorMessage("Invalid number of arguments. 3 arguments passed."),
                TestSpec.forStrategy("Cast strategy", SpecificInputTypeStrategies.CAST)
                        .calledWithArgumentTypes(INT(), BIGINT())
                        .calledWithLiteralAt(1, BIGINT())
                        .expectSignature("f(<ANY>, <TYPE LITERAL>)")
                        .expectArgumentTypes(INT(), BIGINT()),
                TestSpec.forStrategy(
                                "Cast strategy for invalid target type",
                                SpecificInputTypeStrategies.CAST)
                        .calledWithArgumentTypes(BOOLEAN(), DATE())
                        .calledWithLiteralAt(1, DATE())
                        .expectErrorMessage("Unsupported cast from 'BOOLEAN' to 'DATE'."),
                TestSpec.forStrategy(
                                "Logical type roots instead of concrete data types",
                                sequence(
                                        logical(LogicalTypeRoot.VARCHAR),
                                        logical(LogicalTypeRoot.DECIMAL, true),
                                        logical(LogicalTypeRoot.DECIMAL),
                                        logical(LogicalTypeRoot.BOOLEAN),
                                        logical(LogicalTypeRoot.INTEGER, false),
                                        logical(LogicalTypeRoot.INTEGER)))
                        .calledWithArgumentTypes(
                                NULL(),
                                INT(),
                                DOUBLE(),
                                BOOLEAN().notNull(),
                                INT().notNull(),
                                INT().notNull())
                        .expectSignature(
                                "f(<VARCHAR>, <DECIMAL NULL>, <DECIMAL>, <BOOLEAN>, <INTEGER NOT NULL>, <INTEGER>)")
                        .expectArgumentTypes(
                                VARCHAR(1),
                                DECIMAL(10, 0),
                                DECIMAL(30, 15),
                                BOOLEAN().notNull(),
                                INT().notNull(),
                                INT().notNull()),
                TestSpec.forStrategy(
                                "Logical type roots with wrong implicit cast",
                                sequence(logical(LogicalTypeRoot.VARCHAR)))
                        .calledWithArgumentTypes(INT())
                        .expectSignature("f(<VARCHAR>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type root 'VARCHAR' but actual type was 'INT'."),
                TestSpec.forStrategy(
                                "Logical type roots with wrong nullability",
                                sequence(logical(LogicalTypeRoot.VARCHAR, false)))
                        .calledWithArgumentTypes(VARCHAR(5))
                        .expectSignature("f(<VARCHAR NOT NULL>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL type of root 'VARCHAR' but actual type was 'VARCHAR(5)'."),
                TestSpec.forStrategy(
                                "Logical type family instead of concrete data types",
                                sequence(
                                        logical(LogicalTypeFamily.CHARACTER_STRING, true),
                                        logical(LogicalTypeFamily.EXACT_NUMERIC),
                                        logical(LogicalTypeFamily.EXACT_NUMERIC, true),
                                        logical(LogicalTypeFamily.APPROXIMATE_NUMERIC),
                                        logical(LogicalTypeFamily.APPROXIMATE_NUMERIC),
                                        logical(LogicalTypeFamily.APPROXIMATE_NUMERIC, false)))
                        .calledWithArgumentTypes(
                                NULL(),
                                TINYINT(),
                                SMALLINT().notNull(),
                                INT(),
                                BIGINT().notNull(),
                                DECIMAL(10, 2).notNull())
                        .expectSignature(
                                "f(<CHARACTER_STRING NULL>, <EXACT_NUMERIC>, <EXACT_NUMERIC NULL>, <APPROXIMATE_NUMERIC>, <APPROXIMATE_NUMERIC>, <APPROXIMATE_NUMERIC NOT NULL>)")
                        .expectArgumentTypes(
                                VARCHAR(1),
                                TINYINT(),
                                SMALLINT(),
                                DOUBLE(), // widening with preserved nullability
                                DOUBLE().notNull(), // widening with preserved nullability
                                DOUBLE().notNull()),
                TestSpec.forStrategy(
                                "Logical type family with invalid nullability",
                                sequence(logical(LogicalTypeFamily.EXACT_NUMERIC, false)))
                        .calledWithArgumentTypes(INT())
                        .expectSignature("f(<EXACT_NUMERIC NOT NULL>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL type of family 'EXACT_NUMERIC' but actual type was 'INT'."),
                TestSpec.forStrategy(
                                "Logical type family with invalid type",
                                sequence(logical(LogicalTypeFamily.EXACT_NUMERIC)))
                        .calledWithArgumentTypes(FLOAT())
                        .expectSignature("f(<EXACT_NUMERIC>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type of family 'EXACT_NUMERIC' but actual type was 'FLOAT'."),
                TestSpec.forStrategy(
                                "Constraint argument type strategy",
                                sequence(
                                        and(
                                                explicit(BOOLEAN()),
                                                constraint(
                                                        "%s must be nullable.",
                                                        args ->
                                                                args.get(0)
                                                                        .getLogicalType()
                                                                        .isNullable()))))
                        .calledWithArgumentTypes(BOOLEAN())
                        .expectSignature("f([BOOLEAN & <CONSTRAINT>])")
                        .expectArgumentTypes(BOOLEAN()),
                TestSpec.forStrategy(
                                "Constraint argument type strategy invalid",
                                sequence(
                                        and(
                                                explicit(BOOLEAN().notNull()),
                                                constraint(
                                                        "My constraint says %s must be nullable.",
                                                        args ->
                                                                args.get(0)
                                                                        .getLogicalType()
                                                                        .isNullable()))))
                        .calledWithArgumentTypes(BOOLEAN().notNull())
                        .expectErrorMessage(
                                "My constraint says BOOLEAN NOT NULL must be nullable."),
                TestSpec.forStrategy(
                                "Composite type strategy with ROW",
                                sequence(InputTypeStrategies.COMPOSITE))
                        .calledWithArgumentTypes(ROW(FIELD("f0", BIGINT())))
                        .expectSignature("f(<COMPOSITE>)")
                        .expectArgumentTypes(ROW(FIELD("f0", BIGINT()))),
                TestSpec.forStrategy(
                                "Composite type strategy with STRUCTURED type",
                                sequence(InputTypeStrategies.COMPOSITE))
                        .calledWithArgumentTypes(DataTypes.of(SimpleStructuredType.class).notNull())
                        .expectSignature("f(<COMPOSITE>)")
                        .expectArgumentTypes(DataTypes.of(SimpleStructuredType.class).notNull()),
                TestSpec.forStrategy(
                                "Same named arguments for overloaded method.",
                                or(sequence(explicit(STRING())), sequence(explicit(INT()))))
                        .namedArguments("sameName")
                        .calledWithArgumentTypes(BOOLEAN())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(STRING)\nf(INT)"),
                TestSpec.forStrategy(
                                "Common argument type strategy",
                                sequence(
                                        InputTypeStrategies.COMMON_ARG,
                                        InputTypeStrategies.COMMON_ARG))
                        .calledWithArgumentTypes(INT(), BIGINT())
                        .expectSignature("f(<COMMON>, <COMMON>)")
                        .expectArgumentTypes(BIGINT(), BIGINT()),
                TestSpec.forStrategy(
                                "ArrayElement argument type strategy",
                                sequence(
                                        logical(LogicalTypeRoot.ARRAY),
                                        SpecificInputTypeStrategies.ARRAY_ELEMENT_ARG))
                        .calledWithArgumentTypes(ARRAY(INT().notNull()).notNull(), INT())
                        .expectSignature("f(<ARRAY>, <ARRAY ELEMENT>)")
                        .expectArgumentTypes(ARRAY(INT().notNull()).notNull(), INT()),
                TestSpec.forStrategy(sequence(SpecificInputTypeStrategies.ARRAY_FULLY_COMPARABLE))
                        .expectSignature("f(<ARRAY<COMPARABLE>>)")
                        .calledWithArgumentTypes(ARRAY(ROW()))
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "f(<ARRAY<COMPARABLE>>)"),
                TestSpec.forStrategy(
                                "Strategy fails if input argument type is not ARRAY",
                                sequence(SpecificInputTypeStrategies.ARRAY_FULLY_COMPARABLE))
                        .calledWithArgumentTypes(INT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "f(<ARRAY<COMPARABLE>>)"),
                TestSpec.forStrategy(
                                "PROCTIME type strategy",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.PROCTIME))
                        .calledWithArgumentTypes(timeIndicatorType(TimestampKind.PROCTIME))
                        .expectSignature("f(<WINDOW REFERENCE>)")
                        .expectArgumentTypes(timeIndicatorType(TimestampKind.PROCTIME)),
                TestSpec.forStrategy(
                                "PROCTIME type strategy on non time indicator",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.PROCTIME))
                        .calledWithArgumentTypes(BIGINT())
                        .expectErrorMessage("Reference to a rowtime or proctime window required."),
                TestSpec.forStrategy(
                                "ROWTIME type strategy",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.ROWTIME))
                        .calledWithArgumentTypes(timeIndicatorType(TimestampKind.ROWTIME))
                        .expectSignature("f(<WINDOW REFERENCE>)")
                        .expectArgumentTypes(timeIndicatorType(TimestampKind.ROWTIME)),
                TestSpec.forStrategy(
                                "ROWTIME type strategy on proctime indicator",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.ROWTIME))
                        .calledWithArgumentTypes(timeIndicatorType(TimestampKind.PROCTIME))
                        .expectErrorMessage(
                                "A proctime window cannot provide a rowtime attribute."),
                TestSpec.forStrategy(
                                "PROCTIME type strategy on rowtime indicator",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.PROCTIME))
                        .calledWithArgumentTypes(timeIndicatorType(TimestampKind.ROWTIME))
                        .expectArgumentTypes(timeIndicatorType(TimestampKind.PROCTIME)),
                TestSpec.forStrategy(
                                "ROWTIME type strategy on long in batch mode",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.ROWTIME))
                        .calledWithArgumentTypes(BIGINT())
                        .expectArgumentTypes(BIGINT()),
                TestSpec.forStrategy(
                                "ROWTIME type strategy on non time attribute",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.ROWTIME))
                        .calledWithArgumentTypes(SMALLINT())
                        .expectErrorMessage("Reference to a rowtime or proctime window required."),
                TestSpec.forStrategy(
                                "PROCTIME type strategy on non time attribute",
                                SpecificInputTypeStrategies.windowTimeIndicator(
                                        TimestampKind.PROCTIME))
                        .calledWithArgumentTypes(SMALLINT())
                        .expectErrorMessage("Reference to a rowtime or proctime window required."),
                TestSpec.forStrategy(
                                "Reinterpret_cast strategy",
                                SpecificInputTypeStrategies.REINTERPRET_CAST)
                        .calledWithArgumentTypes(DATE(), BIGINT(), BOOLEAN().notNull())
                        .calledWithLiteralAt(1, BIGINT())
                        .calledWithLiteralAt(2, true)
                        .expectSignature("f(<ANY>, <TYPE LITERAL>, <TRUE | FALSE>)")
                        .expectArgumentTypes(DATE(), BIGINT(), BOOLEAN().notNull()),
                TestSpec.forStrategy(
                                "Reinterpret_cast strategy non literal overflow",
                                SpecificInputTypeStrategies.REINTERPRET_CAST)
                        .calledWithArgumentTypes(DATE(), BIGINT(), BOOLEAN().notNull())
                        .calledWithLiteralAt(1, BIGINT())
                        .expectErrorMessage("Not null boolean literal expected for overflow."),
                TestSpec.forStrategy(
                                "Reinterpret_cast strategy not supported cast",
                                SpecificInputTypeStrategies.REINTERPRET_CAST)
                        .calledWithArgumentTypes(INT(), BIGINT(), BOOLEAN().notNull())
                        .calledWithLiteralAt(1, BIGINT())
                        .calledWithLiteralAt(2, true)
                        .expectErrorMessage("Unsupported reinterpret cast from 'INT' to 'BIGINT'"),
                TestSpec.forStrategy("IndexArgumentTypeStrategy", sequence(INDEX))
                        .calledWithArgumentTypes(TINYINT())
                        .expectSignature("f(<INTEGER_NUMERIC>)")
                        .expectArgumentTypes(TINYINT()),
                TestSpec.forStrategy("IndexArgumentTypeStrategy", sequence(INDEX))
                        .calledWithArgumentTypes(INT())
                        .calledWithLiteralAt(0)
                        .expectArgumentTypes(INT()),
                TestSpec.forStrategy("IndexArgumentTypeStrategy BIGINT support", sequence(INDEX))
                        .calledWithArgumentTypes(BIGINT().notNull())
                        .calledWithLiteralAt(0, Long.MAX_VALUE)
                        .expectArgumentTypes(BIGINT().notNull()),
                TestSpec.forStrategy("IndexArgumentTypeStrategy index range", sequence(INDEX))
                        .calledWithArgumentTypes(INT().notNull())
                        .calledWithLiteralAt(0, -1)
                        .expectErrorMessage(
                                "Index must be an integer starting from '0', but was '-1'."),
                TestSpec.forStrategy("IndexArgumentTypeStrategy index type", sequence(INDEX))
                        .calledWithArgumentTypes(DECIMAL(10, 5))
                        .expectErrorMessage("Index can only be an INTEGER NUMERIC type."),

                // Percentage ArgumentStrategy
                TestSpec.forStrategy("normal", sequence(percentage(true)))
                        .calledWithArgumentTypes(DOUBLE())
                        .expectSignature("f(<NUMERIC>)")
                        .expectArgumentTypes(DOUBLE()),
                TestSpec.forStrategy("implicit cast", sequence(percentage(false)))
                        .calledWithArgumentTypes(DECIMAL(5, 2).notNull())
                        .expectSignature("f(<NUMERIC NOT NULL>)")
                        .expectArgumentTypes(DOUBLE().notNull()),
                TestSpec.forStrategy("literal", sequence(percentage(true)))
                        .calledWithArgumentTypes(DECIMAL(2, 2))
                        .calledWithLiteralAt(0, BigDecimal.valueOf(45, 2))
                        .expectArgumentTypes(DOUBLE()),
                TestSpec.forStrategy("literal", sequence(percentage(false)))
                        .calledWithArgumentTypes(INT().notNull())
                        .calledWithLiteralAt(0, 1)
                        .expectArgumentTypes(DOUBLE().notNull()),
                TestSpec.forStrategy("invalid type", sequence(percentage(true)))
                        .calledWithArgumentTypes(STRING())
                        .expectErrorMessage("Percentage must be of NUMERIC type."),
                TestSpec.forStrategy("invalid nullability", sequence(percentage(false)))
                        .calledWithArgumentTypes(DOUBLE())
                        .expectErrorMessage("Percentage must be of NOT NULL type."),
                TestSpec.forStrategy("invalid literal value", sequence(percentage(false)))
                        .calledWithArgumentTypes(DECIMAL(2, 1).notNull())
                        .calledWithLiteralAt(0, BigDecimal.valueOf(20, 1))
                        .expectErrorMessage(
                                "Percentage must be between [0.0, 1.0], but was '2.0'."),
                TestSpec.forStrategy("invalid literal value", sequence(percentage(false)))
                        .calledWithArgumentTypes(DECIMAL(2, 1).notNull())
                        .calledWithLiteralAt(0, BigDecimal.valueOf(-5, 1))
                        .expectErrorMessage(
                                "Percentage must be between [0.0, 1.0], but was '-0.5'."),

                // Percentage Array ArgumentStrategy
                TestSpec.forStrategy("normal", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(DOUBLE()))
                        .expectSignature("f(ARRAY<NUMERIC>)")
                        .expectArgumentTypes(ARRAY(DOUBLE())),
                TestSpec.forStrategy("implicit cast", sequence(percentageArray(false)))
                        .calledWithArgumentTypes(ARRAY(DECIMAL(5, 2).notNull()).notNull())
                        .expectSignature("f(ARRAY<NUMERIC NOT NULL> NOT NULL)")
                        .expectArgumentTypes(ARRAY(DOUBLE().notNull()).notNull()),
                TestSpec.forStrategy("literal", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(DOUBLE()))
                        .calledWithLiteralAt(0, new Double[] {0.45, 0.55})
                        .expectArgumentTypes(ARRAY(DOUBLE())),
                TestSpec.forStrategy("literal", sequence(percentageArray(false)))
                        .calledWithArgumentTypes(ARRAY(DECIMAL(2, 2).notNull()).notNull())
                        .calledWithLiteralAt(
                                0,
                                new BigDecimal[] {
                                    BigDecimal.valueOf(45, 2), BigDecimal.valueOf(55, 2)
                                })
                        .expectArgumentTypes(ARRAY(DOUBLE().notNull()).notNull()),
                TestSpec.forStrategy("literal", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(INT()))
                        .calledWithLiteralAt(0, new Integer[] {0, 1})
                        .expectArgumentTypes(ARRAY(DOUBLE())),
                TestSpec.forStrategy("empty literal array", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(DOUBLE()))
                        .calledWithLiteralAt(0, new Double[0])
                        .expectArgumentTypes(ARRAY(DOUBLE())),
                TestSpec.forStrategy("not array", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(DOUBLE())
                        .expectErrorMessage("Percentage must be an array."),
                TestSpec.forStrategy("invalid array nullability", sequence(percentageArray(false)))
                        .calledWithArgumentTypes(ARRAY(STRING().notNull()))
                        .expectErrorMessage("Percentage must be a non-null array."),
                TestSpec.forStrategy("invalid element type", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(STRING()))
                        .expectErrorMessage(
                                "Value in the percentage array must be of NUMERIC type."),
                TestSpec.forStrategy(
                                "invalid element nullability", sequence(percentageArray(false)))
                        .calledWithArgumentTypes(ARRAY(DOUBLE()).notNull())
                        .expectErrorMessage(
                                "Value in the percentage array must be of NOT NULL type."),
                TestSpec.forStrategy("invalid literal", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(DOUBLE()))
                        .calledWithLiteralAt(0, new Double[] {0.5, 1.5})
                        .expectErrorMessage(
                                "Value in the percentage array must be between [0.0, 1.0], but was '1.5'."),
                TestSpec.forStrategy("invalid literal", sequence(percentageArray(true)))
                        .calledWithArgumentTypes(ARRAY(DECIMAL(3, 2)))
                        .calledWithLiteralAt(
                                0,
                                new BigDecimal[] {
                                    BigDecimal.valueOf(-1, 1), BigDecimal.valueOf(5, 1)
                                })
                        .expectErrorMessage(
                                "Value in the percentage array must be between [0.0, 1.0], but was '-0.1'."));
    }

    private Stream<TestSpec> arrayOfArgumentTestCases() {
        return Stream.of(
                // ArrayOfRoot
                TestSpec.forStrategy(
                                "Array of logical element type roots instead of concrete data types",
                                sequence(
                                        arrayOf(LogicalTypeRoot.VARCHAR),
                                        arrayOf(LogicalTypeRoot.INTEGER),
                                        arrayOf(LogicalTypeRoot.DECIMAL, true, null),
                                        arrayOf(LogicalTypeRoot.BOOLEAN, null, false),
                                        arrayOf(LogicalTypeRoot.INTEGER, false, true)))
                        .calledWithArgumentTypes(
                                ARRAY(NULL()),
                                ARRAY(INT().notNull()),
                                ARRAY(INT()),
                                ARRAY(BOOLEAN().notNull()).notNull(),
                                ARRAY(INT().notNull()).notNull())
                        .expectSignature(
                                "f(ARRAY<VARCHAR>, ARRAY<INTEGER>, ARRAY<DECIMAL> NULL, ARRAY<BOOLEAN NOT NULL>, ARRAY<INTEGER NULL> NOT NULL)")
                        .expectArgumentTypes(
                                ARRAY(VARCHAR(1)),
                                ARRAY(INT().notNull()),
                                ARRAY(DECIMAL(10, 0)),
                                ARRAY(BOOLEAN().notNull()).notNull(),
                                ARRAY(INT()).notNull()),
                TestSpec.forStrategy("Not an array", sequence(arrayOf(LogicalTypeRoot.VARCHAR)))
                        .calledWithArgumentTypes(VARCHAR(5))
                        .expectSignature("f(ARRAY<VARCHAR>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type of root 'ARRAY' but actual type was 'VARCHAR(5)'."),
                TestSpec.forStrategy(
                                "Array with wrong nullability",
                                sequence(arrayOf(LogicalTypeRoot.VARCHAR, false, null)))
                        .calledWithArgumentTypes(ARRAY(VARCHAR(5)))
                        .expectSignature("f(ARRAY<VARCHAR> NOT NULL)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL type of root 'ARRAY' but actual type was 'ARRAY<VARCHAR(5)>'."),
                TestSpec.forStrategy(
                                "Element with wrong nullability",
                                sequence(arrayOf(LogicalTypeRoot.VARCHAR, null, false)))
                        .calledWithArgumentTypes(ARRAY(VARCHAR(5)))
                        .expectSignature("f(ARRAY<VARCHAR NOT NULL>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL element type of root 'VARCHAR' but actual type was 'VARCHAR(5)'."),
                TestSpec.forStrategy(
                                "Array of logical element type roots with wrong implicit cast",
                                sequence(arrayOf(LogicalTypeRoot.VARCHAR)))
                        .calledWithArgumentTypes(ARRAY(INT()))
                        .expectSignature("f(ARRAY<VARCHAR>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected element type of root 'VARCHAR' but actual type was 'INT'."),
                // ArrayOfFamily
                TestSpec.forStrategy(
                                "Array of logical element type family instead of concrete data types",
                                sequence(
                                        arrayOf(LogicalTypeFamily.CHARACTER_STRING),
                                        arrayOf(LogicalTypeFamily.EXACT_NUMERIC),
                                        arrayOf(LogicalTypeFamily.APPROXIMATE_NUMERIC, null, true),
                                        arrayOf(LogicalTypeFamily.APPROXIMATE_NUMERIC, false, null),
                                        arrayOf(LogicalTypeFamily.APPROXIMATE_NUMERIC, null, false),
                                        arrayOf(LogicalTypeFamily.EXACT_NUMERIC, true, true)))
                        .calledWithArgumentTypes(
                                ARRAY(NULL()),
                                ARRAY(TINYINT().notNull()),
                                ARRAY(FLOAT()),
                                ARRAY(INT().notNull()).notNull(),
                                ARRAY(BIGINT().notNull()).notNull(),
                                ARRAY(DECIMAL(10, 2).notNull()).notNull())
                        .expectSignature(
                                "f(ARRAY<<CHARACTER_STRING>>, ARRAY<<EXACT_NUMERIC>>, ARRAY<<APPROXIMATE_NUMERIC NULL>>, ARRAY<<APPROXIMATE_NUMERIC>> NOT NULL, ARRAY<<APPROXIMATE_NUMERIC NOT NULL>>, ARRAY<<EXACT_NUMERIC NULL>> NULL)")
                        .expectArgumentTypes(
                                ARRAY(VARCHAR(1)),
                                ARRAY(TINYINT().notNull()),
                                ARRAY(FLOAT()),
                                ARRAY(DOUBLE().notNull()).notNull(),
                                ARRAY(DOUBLE().notNull()).notNull(),
                                ARRAY(DECIMAL(10, 2))),
                TestSpec.forStrategy(
                                "Not an array", sequence(arrayOf(LogicalTypeFamily.BINARY_STRING)))
                        .calledWithArgumentTypes(BYTES())
                        .expectSignature("f(ARRAY<<BINARY_STRING>>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected type of root 'ARRAY' but actual type was 'BYTES'."),
                TestSpec.forStrategy(
                                "Array with wrong nullability",
                                sequence(arrayOf(LogicalTypeFamily.CHARACTER_STRING, false, null)))
                        .calledWithArgumentTypes(ARRAY(STRING()))
                        .expectSignature("f(ARRAY<<CHARACTER_STRING>> NOT NULL)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL type of root 'ARRAY' but actual type was 'ARRAY<STRING>'."),
                TestSpec.forStrategy(
                                "Element with wrong nullability",
                                sequence(arrayOf(LogicalTypeFamily.BINARY_STRING, null, false)))
                        .calledWithArgumentTypes(ARRAY(BYTES()))
                        .expectSignature("f(ARRAY<<BINARY_STRING NOT NULL>>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL element type of family 'BINARY_STRING' but actual type was 'BYTES'."),
                TestSpec.forStrategy(
                                "Array of logical element type family with invalid type",
                                sequence(arrayOf(LogicalTypeFamily.EXACT_NUMERIC)))
                        .calledWithArgumentTypes(ARRAY(FLOAT()))
                        .expectSignature("f(ARRAY<<EXACT_NUMERIC>>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected element type of family 'EXACT_NUMERIC' but actual type was 'FLOAT'."),
                TestSpec.forStrategy(
                                "Array of logical element type family without a smallest precision type",
                                sequence(arrayOf(LogicalTypeFamily.DATETIME, null, false)))
                        .calledWithArgumentTypes(ARRAY(BIGINT().notNull()))
                        .expectSignature("f(ARRAY<<DATETIME NOT NULL>>)")
                        .expectErrorMessage(
                                "Unsupported argument type. Expected NOT NULL element type of family 'DATETIME' but actual type was 'BIGINT NOT NULL'."));
    }

    private static DataType timeIndicatorType(TimestampKind timestampKind) {
        return TypeConversions.fromLogicalToDataType(
                new LocalZonedTimestampType(false, timestampKind, 3));
    }

    /** Simple pojo that should be converted to a Structured type. */
    public static class SimpleStructuredType {
        public long f0;
    }
}
