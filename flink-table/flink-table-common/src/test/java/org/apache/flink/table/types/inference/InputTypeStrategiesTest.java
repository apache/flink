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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.runners.Parameterized.Parameters;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL_OR_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.OUTPUT_IF_NULL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.WILDCARD;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.constraint;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicitSequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;

/**
 * Tests for built-in {@link InputTypeStrategies}.
 */
public class InputTypeStrategiesTest extends InputTypeStrategiesTestBase {

	@Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return asList(
			// wildcard with 2 arguments
			TestSpec
				.forStrategy(WILDCARD)
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT())
				.expectSignature("f(*)")
				.expectArgumentTypes(DataTypes.INT(), DataTypes.INT()),

			// wildcard with 0 arguments
			TestSpec
				.forStrategy(WILDCARD)
				.calledWithArgumentTypes()
				.expectSignature("f(*)")
				.expectArgumentTypes(),

			// explicit sequence
			TestSpec
				.forStrategy(explicitSequence(DataTypes.INT().bridgedTo(int.class), DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.BOOLEAN())
				.expectSignature("f(INT, BOOLEAN)")
				.expectArgumentTypes(DataTypes.INT().bridgedTo(int.class), DataTypes.BOOLEAN()),

			// explicit sequence with ROW ignoring field names
			TestSpec
				.forStrategy(explicitSequence(DataTypes.ROW(DataTypes.FIELD("expected", DataTypes.INT()))))
				.calledWithArgumentTypes(DataTypes.ROW(DataTypes.FIELD("actual", DataTypes.INT())))
				.expectSignature("f(ROW<`expected` INT>)")
				.expectArgumentTypes(DataTypes.ROW(DataTypes.FIELD("expected", DataTypes.INT()))),

			// invalid named sequence
			TestSpec
				.forStrategy(
					explicitSequence(
						new String[]{"i", "s"},
						new DataType[]{DataTypes.INT(), DataTypes.STRING()}))
				.calledWithArgumentTypes(DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING)"),

			// incompatible nullability
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT().notNull()))
				.calledWithArgumentTypes(DataTypes.BIGINT())
				.expectErrorMessage("Unsupported argument type. Expected type 'BIGINT NOT NULL' but actual type was 'BIGINT'."),

			// implicit cast
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT()))
				.calledWithArgumentTypes(DataTypes.INT())
				.expectArgumentTypes(DataTypes.BIGINT()),

			// incompatible types
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT()))
				.calledWithArgumentTypes(DataTypes.STRING())
				.expectErrorMessage("Unsupported argument type. Expected type 'BIGINT' but actual type was 'STRING'."),

			// incompatible number of arguments
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BIGINT(), DataTypes.BIGINT()))
				.calledWithArgumentTypes(DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. At least 2 arguments expected but 1 passed."),

			// any type
			TestSpec
				.forStrategy(sequence(ANY))
				.calledWithArgumentTypes(DataTypes.BIGINT())
				.expectSignature("f(<ANY>)")
				.expectArgumentTypes(DataTypes.BIGINT()),

			// incompatible number of arguments
			TestSpec
				.forStrategy(sequence(ANY))
				.calledWithArgumentTypes(DataTypes.BIGINT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. At most 1 arguments expected but 2 passed."),

			TestSpec
				.forStrategy(
					"OR with bridging class",
					or(
						explicitSequence(DataTypes.STRING()),
						explicitSequence(DataTypes.INT().bridgedTo(int.class)),
						explicitSequence(DataTypes.BOOLEAN())))
				.calledWithArgumentTypes(DataTypes.INT())
				.calledWithArgumentTypes(DataTypes.TINYINT())
				.expectSignature("f(STRING)\nf(INT)\nf(BOOLEAN)")
				.expectArgumentTypes(DataTypes.INT().bridgedTo(int.class)),

			TestSpec
				.forStrategy(
					"OR with implicit casting",
					or(
						explicitSequence(DataTypes.TINYINT()),
						explicitSequence(DataTypes.INT()),
						explicitSequence(DataTypes.BIGINT())))
				.calledWithArgumentTypes(DataTypes.SMALLINT())
				.expectArgumentTypes(DataTypes.INT()),

			TestSpec
				.forStrategy(
					"OR with implicit casting of null",
					or(
						explicitSequence(DataTypes.STRING().notNull()),
						explicitSequence(DataTypes.INT().notNull()),
						explicitSequence(DataTypes.BIGINT())))
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectArgumentTypes(DataTypes.BIGINT()),

			TestSpec
				.forStrategy(
					"OR with implicit casting using first match",
					or(
						explicitSequence(DataTypes.VARCHAR(20)),
						explicitSequence(DataTypes.VARCHAR(10))))
				.calledWithArgumentTypes(DataTypes.VARCHAR(1))
				.expectArgumentTypes(DataTypes.VARCHAR(20)),

			TestSpec
				.forStrategy(
					"OR with invalid implicit casting of null",
					or(
						explicitSequence(DataTypes.STRING().notNull()),
						explicitSequence(DataTypes.INT().notNull()),
						explicitSequence(DataTypes.BIGINT().notNull())))
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\n" +
					"f(STRING NOT NULL)\nf(INT NOT NULL)\nf(BIGINT NOT NULL)"),

			TestSpec
				.forStrategy(
					"OR with invalid type",
					or(explicitSequence(DataTypes.INT()), explicitSequence(DataTypes.STRING())))
				.calledWithArgumentTypes(DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT)\nf(STRING)"),

			// invalid typed sequence
			TestSpec
				.forStrategy(explicitSequence(DataTypes.INT(), DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT, BOOLEAN)"),

			// sequence with wildcard
			TestSpec
				.forStrategy(sequence(ANY, explicit(DataTypes.INT())))
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.INT())
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.TINYINT())
				.expectArgumentTypes(DataTypes.BOOLEAN(), DataTypes.INT()),

			// invalid named sequence
			TestSpec
				.forStrategy(sequence(
					new String[]{"any", "int"},
					new ArgumentTypeStrategy[]{ANY, explicit(DataTypes.INT())}))
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(any <ANY>, int INT)"),

			// sequence with OR and implicit casting
			TestSpec
				.forStrategy(
					sequence(
						explicit(DataTypes.INT()),
						or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))))
				.expectSignature("f(INT, [BOOLEAN | INT])")
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT())
				.calledWithArgumentTypes(DataTypes.TINYINT(), DataTypes.TINYINT())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.INT()),

			// sequence with OR
			TestSpec
				.forStrategy(
					sequence(
						explicit(DataTypes.INT()),
						or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.STRING()))))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(INT, [BOOLEAN | STRING])"),

			// sequence with literal
			TestSpec
				.forStrategy(sequence(LITERAL))
				.calledWithLiteralAt(0)
				.calledWithArgumentTypes(DataTypes.INT())
				.expectArgumentTypes(DataTypes.INT()),

			// sequence with literal
			TestSpec
				.forStrategy(sequence(and(LITERAL, explicit(DataTypes.STRING())), explicit(DataTypes.INT())))
				.calledWithLiteralAt(0)
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.INT())
				.expectSignature("f([<LITERAL NOT NULL> & STRING], INT)")
				.expectArgumentTypes(DataTypes.STRING(), DataTypes.INT()),

			// sequence with missing literal
			TestSpec
				.forStrategy(sequence(and(explicit(DataTypes.STRING()), LITERAL_OR_NULL), explicit(DataTypes.INT())))
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.INT())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf([STRING & <LITERAL>], INT)"),

			// vararg sequence
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(
					DataTypes.INT(),
					DataTypes.STRING(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN())
				.expectArgumentTypes(
					DataTypes.INT(),
					DataTypes.STRING(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN()),

			// vararg sequence with conversion class
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"var"},
						new ArgumentTypeStrategy[]{explicit(DataTypes.BOOLEAN().bridgedTo(boolean.class))}))
				.calledWithArgumentTypes(
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN(),
					DataTypes.BOOLEAN())
				.expectSignature("f(var BOOLEAN...)")
				.expectArgumentTypes(
					DataTypes.BOOLEAN().bridgedTo(boolean.class),
					DataTypes.BOOLEAN().bridgedTo(boolean.class),
					DataTypes.BOOLEAN().bridgedTo(boolean.class)),

			// vararg sequence
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING()),

			// invalid vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var BOOLEAN...)"),

			// invalid non-vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							explicit(DataTypes.BOOLEAN())}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.INT(), DataTypes.BOOLEAN())
				.expectErrorMessage("Unsupported argument type. Expected type 'STRING' but actual type was 'INT'."),

			// OR in vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BOOLEAN())
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT(), DataTypes.BOOLEAN()),

			// invalid OR in vararg type
			TestSpec
				.forStrategy(
					varyingSequence(
						new String[]{"i", "s", "var"},
						new ArgumentTypeStrategy[]{
							explicit(DataTypes.INT()),
							explicit(DataTypes.STRING()),
							or(explicit(DataTypes.BOOLEAN()), explicit(DataTypes.INT()))}))
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(i INT, s STRING, var [BOOLEAN | INT]...)"),

			// incomplete inference
			TestSpec
				.forStrategy(WILDCARD)
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectSignature("f(*)")
				.expectArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL()),

			// typed arguments help inferring a type
			TestSpec
				.forStrategy(WILDCARD)
				.typedArguments(DataTypes.INT().bridgedTo(int.class), DataTypes.STRING(), DataTypes.BOOLEAN())
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectArgumentTypes(DataTypes.INT().bridgedTo(int.class), DataTypes.STRING(), DataTypes.BOOLEAN()),

			// surrounding function helps inferring a type
			TestSpec
				.forStrategy(sequence(OUTPUT_IF_NULL, OUTPUT_IF_NULL, OUTPUT_IF_NULL))
				.surroundingStrategy(explicitSequence(DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.NULL(), DataTypes.STRING(), DataTypes.NULL())
				.expectSignature("f(<OUTPUT>, <OUTPUT>, <OUTPUT>)")
				.expectArgumentTypes(DataTypes.BOOLEAN(), DataTypes.STRING(), DataTypes.BOOLEAN()),

			// surrounding function helps inferring a type
			TestSpec
				.forStrategy(sequence(or(OUTPUT_IF_NULL, explicit(DataTypes.INT()))))
				.surroundingStrategy(explicitSequence(DataTypes.BOOLEAN()))
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectSignature("f([<OUTPUT> | INT])")
				.expectArgumentTypes(DataTypes.BOOLEAN()),

			// surrounding info can not infer input type and does not help inferring a type
			TestSpec
				.forStrategy(explicitSequence(DataTypes.BOOLEAN()))
				.surroundingStrategy(WILDCARD)
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectSignature("f(BOOLEAN)")
				.expectArgumentTypes(DataTypes.BOOLEAN()),

			// surrounding function does not help inferring a type
			TestSpec
				.forStrategy(sequence(or(OUTPUT_IF_NULL, explicit(DataTypes.INT()))))
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectSignature("f([<OUTPUT> | INT])")
				.expectArgumentTypes(DataTypes.INT()),

			// typed arguments only with casting
			TestSpec
				.forStrategy(WILDCARD)
				.typedArguments(DataTypes.INT(), DataTypes.STRING())
				.calledWithArgumentTypes(DataTypes.TINYINT(), DataTypes.STRING())
				.expectSignature("f(INT, STRING)")
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING()),

			// invalid typed arguments
			TestSpec
				.forStrategy(WILDCARD)
				.typedArguments(DataTypes.INT(), DataTypes.STRING())
				.calledWithArgumentTypes(DataTypes.STRING(), DataTypes.STRING())
				.expectErrorMessage("Invalid argument type at position 0. Data type INT expected but STRING passed."),

			// named arguments
			TestSpec
				.forStrategy(WILDCARD)
				.namedArguments("i", "s")
				.typedArguments(DataTypes.INT(), DataTypes.STRING())
				.expectSignature("f(i => INT, s => STRING)"),

			TestSpec
				.forStrategy(
					"Wildcard with count verifies arguments number",
					InputTypeStrategies.wildcardWithCount(ConstantArgumentCount.from(2)))
				.calledWithArgumentTypes(DataTypes.STRING())
				.expectErrorMessage("Invalid number of arguments. At least 2 arguments expected but 1 passed."),

			TestSpec.forStrategy(
					"Array strategy infers a common type",
					InputTypeStrategies.SPECIFIC_FOR_ARRAY)
				.expectSignature("f(<COMMON>, <COMMON>...)")
				.calledWithArgumentTypes(
					DataTypes.INT().notNull(),
					DataTypes.BIGINT().notNull(),
					DataTypes.DOUBLE(),
					DataTypes.DOUBLE().notNull())
				.expectArgumentTypes(DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DOUBLE()),

			TestSpec.forStrategy(
					"Array strategy fails for no arguments",
					InputTypeStrategies.SPECIFIC_FOR_ARRAY)
				.calledWithArgumentTypes()
				.expectErrorMessage("Invalid number of arguments. At least 1 arguments expected but 0 passed."),

			TestSpec.forStrategy(
					"Array strategy fails for null arguments",
					InputTypeStrategies.SPECIFIC_FOR_ARRAY)
				.calledWithArgumentTypes(DataTypes.NULL())
				.expectErrorMessage("Could not find a common type for arguments: [NULL]"),

			TestSpec.forStrategy(
					"Map strategy infers common types",
					InputTypeStrategies.SPECIFIC_FOR_MAP)
				.calledWithArgumentTypes(
					DataTypes.INT().notNull(),
					DataTypes.DOUBLE(),
					DataTypes.BIGINT().notNull(),
					DataTypes.FLOAT().notNull())
				.expectArgumentTypes(
					DataTypes.BIGINT().notNull(),
					DataTypes.DOUBLE(),
					DataTypes.BIGINT().notNull(),
					DataTypes.DOUBLE()),

			TestSpec.forStrategy(
					"Map strategy fails for no arguments",
					InputTypeStrategies.SPECIFIC_FOR_MAP)
				.calledWithArgumentTypes()
				.expectErrorMessage("Invalid number of arguments. At least 2 arguments expected but 0 passed."),

			TestSpec.forStrategy(
					"Map strategy fails for an odd number of arguments",
					InputTypeStrategies.SPECIFIC_FOR_MAP)
				.calledWithArgumentTypes(DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT())
				.expectErrorMessage("Invalid number of arguments. 3 arguments passed."),

			TestSpec.forStrategy(
					"Cast strategy",
					InputTypeStrategies.SPECIFIC_FOR_CAST)
				.calledWithArgumentTypes(DataTypes.INT(), DataTypes.BIGINT())
				.calledWithLiteralAt(1, DataTypes.BIGINT())
				.expectSignature("f(<ANY>, <TYPE LITERAL>)")
				.expectArgumentTypes(DataTypes.INT(), DataTypes.BIGINT()),

			TestSpec.forStrategy(
					"Cast strategy for invalid target type",
					InputTypeStrategies.SPECIFIC_FOR_CAST)
				.calledWithArgumentTypes(DataTypes.BOOLEAN(), DataTypes.DATE())
				.calledWithLiteralAt(1, DataTypes.DATE())
				.expectErrorMessage("Unsupported cast from 'BOOLEAN' to 'DATE'."),

			TestSpec
				.forStrategy(
					"Logical type roots instead of concrete data types",
					sequence(
						logical(LogicalTypeRoot.VARCHAR),
						logical(LogicalTypeRoot.DECIMAL, true),
						logical(LogicalTypeRoot.DECIMAL),
						logical(LogicalTypeRoot.BOOLEAN),
						logical(LogicalTypeRoot.INTEGER, false),
						logical(LogicalTypeRoot.INTEGER)))
				.calledWithArgumentTypes(
					DataTypes.NULL(),
					DataTypes.INT(),
					DataTypes.DOUBLE(),
					DataTypes.BOOLEAN().notNull(),
					DataTypes.INT().notNull(),
					DataTypes.INT().notNull())
				.expectSignature(
					"f(<VARCHAR>, <DECIMAL NULL>, <DECIMAL>, <BOOLEAN>, <INTEGER NOT NULL>, <INTEGER>)")
				.expectArgumentTypes(
					DataTypes.VARCHAR(1),
					DataTypes.DECIMAL(10, 0),
					DataTypes.DECIMAL(30, 15),
					DataTypes.BOOLEAN().notNull(),
					DataTypes.INT().notNull(),
					DataTypes.INT().notNull()),

			TestSpec
				.forStrategy(
					"Logical type roots with wrong implicit cast",
					sequence(logical(LogicalTypeRoot.VARCHAR)))
				.calledWithArgumentTypes(DataTypes.INT())
				.expectSignature("f(<VARCHAR>)")
				.expectErrorMessage(
					"Unsupported argument type. Expected type root 'VARCHAR' but actual type was 'INT'."),

			TestSpec
				.forStrategy(
					"Logical type roots with wrong nullability",
					sequence(logical(LogicalTypeRoot.VARCHAR, false)))
				.calledWithArgumentTypes(DataTypes.VARCHAR(5))
				.expectSignature("f(<VARCHAR NOT NULL>)")
				.expectErrorMessage(
					"Unsupported argument type. Expected nullable type of root 'VARCHAR' but actual type was 'VARCHAR(5)'."),

			TestSpec
				.forStrategy(
					"Logical type family instead of concrete data types",
					sequence(
						logical(LogicalTypeFamily.CHARACTER_STRING, true),
						logical(LogicalTypeFamily.EXACT_NUMERIC),
						logical(LogicalTypeFamily.APPROXIMATE_NUMERIC),
						logical(LogicalTypeFamily.APPROXIMATE_NUMERIC),
						logical(LogicalTypeFamily.APPROXIMATE_NUMERIC, false)))
				.calledWithArgumentTypes(
					DataTypes.NULL(),
					DataTypes.TINYINT(),
					DataTypes.INT(),
					DataTypes.BIGINT().notNull(),
					DataTypes.DECIMAL(10, 2).notNull())
				.expectSignature(
					"f(<CHARACTER_STRING NULL>, <EXACT_NUMERIC>, <APPROXIMATE_NUMERIC>, <APPROXIMATE_NUMERIC>, <APPROXIMATE_NUMERIC NOT NULL>)")
				.expectArgumentTypes(
					DataTypes.VARCHAR(1),
					DataTypes.TINYINT(),
					DataTypes.DOUBLE(), // widening with preserved nullability
					DataTypes.DOUBLE().notNull(), // widening with preserved nullability
					DataTypes.DOUBLE().notNull()),

			TestSpec
				.forStrategy(
					"Logical type family with invalid type",
					sequence(logical(LogicalTypeFamily.EXACT_NUMERIC)))
				.calledWithArgumentTypes(DataTypes.FLOAT())
				.expectSignature("f(<EXACT_NUMERIC>)")
				.expectErrorMessage(
					"Unsupported argument type. Expected type of family 'EXACT_NUMERIC' but actual type was 'FLOAT'."),

			TestSpec
				.forStrategy(
					"Constraint argument type strategy",
					sequence(
						and(
							explicit(DataTypes.BOOLEAN()),
							constraint(
								"%s must be nullable.",
								args -> args.get(0).getLogicalType().isNullable()))))
				.calledWithArgumentTypes(DataTypes.BOOLEAN())
				.expectSignature("f([BOOLEAN & <CONSTRAINT>])")
				.expectArgumentTypes(DataTypes.BOOLEAN()),

			TestSpec
				.forStrategy(
					"Constraint argument type strategy invalid",
					sequence(
						and(
							explicit(DataTypes.BOOLEAN().notNull()),
							constraint(
								"My constraint says %s must be nullable.",
								args -> args.get(0).getLogicalType().isNullable()))))
				.calledWithArgumentTypes(DataTypes.BOOLEAN().notNull())
				.expectErrorMessage("My constraint says BOOLEAN NOT NULL must be nullable."),

			TestSpec
				.forStrategy(
					"Composite type strategy with ROW",
					sequence(InputTypeStrategies.COMPOSITE)
				)
				.calledWithArgumentTypes(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT())))
				.expectSignature("f(<COMPOSITE>)")
				.expectArgumentTypes(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.BIGINT()))),

			TestSpec
				.forStrategy(
					"Composite type strategy with STRUCTURED type",
					sequence(InputTypeStrategies.COMPOSITE)
				)
				.calledWithArgumentTypes(DataTypes.of(SimpleStructuredType.class).notNull())
				.expectSignature("f(<COMPOSITE>)")
				.expectArgumentTypes(DataTypes.of(SimpleStructuredType.class).notNull()),

			TestSpec
				.forStrategy(
					"Same named arguments for overloaded method.",
					or(
						sequence(explicit(DataTypes.STRING())),
						sequence(explicit(DataTypes.INT()))))
				.namedArguments("sameName")
				.calledWithArgumentTypes(DataTypes.BOOLEAN())
				.expectErrorMessage("Invalid input arguments. Expected signatures are:\nf(STRING)\nf(INT)")
		);
	}

	/**
	 * Simple pojo that should be converted to a Structured type.
	 */
	public static class SimpleStructuredType {
		public long f0;
	}
}
