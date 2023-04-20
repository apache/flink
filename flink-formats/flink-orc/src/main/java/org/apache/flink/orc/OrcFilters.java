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

package org.apache.flink.orc;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.function.TriFunction;

import org.apache.flink.shaded.curator5.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

/** Utility class that provides helper methods to work with Orc Filter PushDown. */
public class OrcFilters {

    private static final Logger LOG = LoggerFactory.getLogger(OrcFilters.class);

    private static final ImmutableMap<FunctionDefinition, Function<CallExpression, Predicate>>
            FILTERS =
                    new ImmutableMap.Builder<
                                    FunctionDefinition, Function<CallExpression, Predicate>>()
                            .put(BuiltInFunctionDefinitions.IS_NULL, OrcFilters::convertIsNull)
                            .put(
                                    BuiltInFunctionDefinitions.IS_NOT_NULL,
                                    OrcFilters::convertIsNotNull)
                            .put(BuiltInFunctionDefinitions.NOT, OrcFilters::convertNot)
                            .put(BuiltInFunctionDefinitions.OR, OrcFilters::convertOr)
                            .put(BuiltInFunctionDefinitions.AND, OrcFilters::convertAnd)
                            .put(
                                    BuiltInFunctionDefinitions.EQUALS,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilters::convertEquals,
                                                    OrcFilters::convertEquals))
                            .put(
                                    BuiltInFunctionDefinitions.NOT_EQUALS,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilters::convertNotEquals,
                                                    OrcFilters::convertNotEquals))
                            .put(
                                    BuiltInFunctionDefinitions.GREATER_THAN,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilters::convertGreaterThan,
                                                    OrcFilters::convertLessThanEquals))
                            .put(
                                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilters::convertGreaterThanEquals,
                                                    OrcFilters::convertLessThan))
                            .put(
                                    BuiltInFunctionDefinitions.LESS_THAN,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilters::convertLessThan,
                                                    OrcFilters::convertGreaterThanEquals))
                            .put(
                                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                    call ->
                                            convertBinary(
                                                    call,
                                                    OrcFilters::convertLessThanEquals,
                                                    OrcFilters::convertGreaterThan))
                            .build();

    private static boolean isRef(Expression expression) {
        return expression instanceof FieldReferenceExpression;
    }

    private static boolean isLit(Expression expression) {
        return expression instanceof ValueLiteralExpression;
    }

    private static boolean isUnaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 1
                && isRef(callExpression.getChildren().get(0));
    }

    private static boolean isBinaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 2
                && (isRef(callExpression.getChildren().get(0))
                                && isLit(callExpression.getChildren().get(1))
                        || isLit(callExpression.getChildren().get(0))
                                && isRef(callExpression.getChildren().get(1)));
    }

    private static Predicate convertIsNull(CallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    callExp);
            return null;
        }

        PredicateLeaf.Type colType =
                toOrcType(
                        ((FieldReferenceExpression) callExp.getChildren().get(0))
                                .getOutputDataType());
        if (colType == null) {
            // unsupported type
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    callExp);
            return null;
        }

        String colName = getColumnName(callExp);

        return new IsNull(colName, colType);
    }

    private static Predicate convertIsNotNull(CallExpression callExp) {
        return new Not(convertIsNull(callExp));
    }

    private static Predicate convertNot(CallExpression callExp) {
        if (callExp.getChildren().size() != 1) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    callExp);
            return null;
        }

        Predicate c = toOrcPredicate(callExp.getChildren().get(0));
        return c == null ? null : new Not(c);
    }

    private static Predicate convertOr(CallExpression callExp) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        Expression left = callExp.getChildren().get(0);
        Expression right = callExp.getChildren().get(1);

        Predicate c1 = toOrcPredicate(left);
        Predicate c2 = toOrcPredicate(right);
        if (c1 == null || c2 == null) {
            return null;
        } else {
            return new Or(c1, c2);
        }
    }

    private static Predicate convertAnd(CallExpression callExp) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        Expression left = callExp.getChildren().get(0);
        Expression right = callExp.getChildren().get(1);

        Predicate c1 = toOrcPredicate(left);
        Predicate c2 = toOrcPredicate(right);
        if (c1 == null || c2 == null) {
            return null;
        } else {
            return new And(c1, c2);
        }
    }

    public static Predicate convertBinary(
            CallExpression callExp,
            TriFunction<String, PredicateLeaf.Type, Serializable, Predicate> func,
            TriFunction<String, PredicateLeaf.Type, Serializable, Predicate> reverseFunc) {
        if (!isBinaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    callExp);
            return null;
        }

        PredicateLeaf.Type litType = getLiteralType(callExp);
        if (litType == null) {
            // unsupported literal type
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    callExp);
            return null;
        }

        String colName = getColumnName(callExp);

        // fetch literal and ensure it is serializable
        Object literalObj = getLiteral(callExp).get();
        Object orcObj = toOrcObject(litType, literalObj);
        Serializable literal;
        // validate that literal is serializable
        if (orcObj instanceof Serializable) {
            literal = (Serializable) orcObj;
        } else {
            LOG.warn(
                    "Encountered a non-serializable literal of type {}. "
                            + "Cannot push predicate [{}] into OrcFileSystemFormatFactory. "
                            + "This is a bug and should be reported.",
                    literalObj.getClass().getCanonicalName(),
                    callExp);
            return null;
        }

        return literalOnRight(callExp)
                ? func.apply(colName, litType, literal)
                : reverseFunc.apply(colName, litType, literal);
    }

    private static Predicate convertEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new Equals(colName, litType, literal);
    }

    private static Predicate convertNotEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new Not(convertEquals(colName, litType, literal));
    }

    private static Predicate convertGreaterThan(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new Not(new LessThanEquals(colName, litType, literal));
    }

    private static Predicate convertGreaterThanEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new Not(new LessThan(colName, litType, literal));
    }

    private static Predicate convertLessThan(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new LessThan(colName, litType, literal);
    }

    private static Predicate convertLessThanEquals(
            String colName, PredicateLeaf.Type litType, Serializable literal) {
        return new LessThanEquals(colName, litType, literal);
    }

    public static Predicate toOrcPredicate(Expression expression) {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            if (FILTERS.get(callExp.getFunctionDefinition()) == null) {
                // unsupported predicate
                LOG.debug(
                        "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                        expression);
                return null;
            }
            return FILTERS.get(callExp.getFunctionDefinition()).apply(callExp);
        } else {
            // unsupported predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    expression);
            return null;
        }
    }

    private static String getColumnName(CallExpression comp) {
        if (literalOnRight(comp)) {
            return ((FieldReferenceExpression) comp.getChildren().get(0)).getName();
        } else {
            return ((FieldReferenceExpression) comp.getChildren().get(1)).getName();
        }
    }

    private static boolean literalOnRight(CallExpression comp) {
        if (comp.getChildren().size() == 1
                && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
            return true;
        } else if (isLit(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
            return false;
        } else if (isRef(comp.getChildren().get(0)) && isLit(comp.getChildren().get(1))) {
            return true;
        } else {
            throw new RuntimeException("Invalid binary comparison.");
        }
    }

    private static PredicateLeaf.Type getLiteralType(CallExpression comp) {
        if (literalOnRight(comp)) {
            return toOrcType(
                    ((ValueLiteralExpression) comp.getChildren().get(1)).getOutputDataType());
        } else {
            return toOrcType(
                    ((ValueLiteralExpression) comp.getChildren().get(0)).getOutputDataType());
        }
    }

    private static Object toOrcObject(PredicateLeaf.Type litType, Object literalObj) {
        switch (litType) {
            case DATE:
                if (literalObj instanceof LocalDate) {
                    LocalDate localDate = (LocalDate) literalObj;
                    return Date.valueOf(localDate);
                } else {
                    return literalObj;
                }
            case TIMESTAMP:
                if (literalObj instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) literalObj;
                    return Timestamp.valueOf(localDateTime);
                } else {
                    return literalObj;
                }
            default:
                return literalObj;
        }
    }

    private static Optional<?> getLiteral(CallExpression comp) {
        if (literalOnRight(comp)) {
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) comp.getChildren().get(1);
            return valueLiteralExpression.getValueAs(
                    valueLiteralExpression.getOutputDataType().getConversionClass());
        } else {
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) comp.getChildren().get(0);
            return valueLiteralExpression.getValueAs(
                    valueLiteralExpression.getOutputDataType().getConversionClass());
        }
    }

    private static PredicateLeaf.Type toOrcType(DataType type) {
        LogicalTypeRoot ltype = type.getLogicalType().getTypeRoot();
        switch (ltype) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return PredicateLeaf.Type.LONG;
            case FLOAT:
            case DOUBLE:
                return PredicateLeaf.Type.FLOAT;
            case BOOLEAN:
                return PredicateLeaf.Type.BOOLEAN;
            case CHAR:
            case VARCHAR:
                return PredicateLeaf.Type.STRING;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return PredicateLeaf.Type.TIMESTAMP;
            case DATE:
                return PredicateLeaf.Type.DATE;
            case DECIMAL:
                return PredicateLeaf.Type.DECIMAL;
            default:
                return null;
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Classes to define predicates
    // --------------------------------------------------------------------------------------------

    /** A filter predicate that can be evaluated by the OrcInputFormat. */
    public abstract static class Predicate implements Serializable {
        public abstract SearchArgument.Builder add(SearchArgument.Builder builder);
    }

    abstract static class ColumnPredicate extends Predicate {
        final String columnName;
        final PredicateLeaf.Type literalType;

        ColumnPredicate(String columnName, PredicateLeaf.Type literalType) {
            this.columnName = columnName;
            this.literalType = literalType;
        }

        Object castLiteral(Serializable literal) {

            switch (literalType) {
                case LONG:
                    if (literal instanceof Byte) {
                        return new Long((Byte) literal);
                    } else if (literal instanceof Short) {
                        return new Long((Short) literal);
                    } else if (literal instanceof Integer) {
                        return new Long((Integer) literal);
                    } else if (literal instanceof Long) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a LONG column requires an integer "
                                        + "literal, i.e., Byte, Short, Integer, or Long.");
                    }
                case FLOAT:
                    if (literal instanceof Float) {
                        return new Double((Float) literal);
                    } else if (literal instanceof Double) {
                        return literal;
                    } else if (literal instanceof BigDecimal) {
                        return ((BigDecimal) literal).doubleValue();
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a FLOAT column requires a floating "
                                        + "literal, i.e., Float or Double.");
                    }
                case STRING:
                    if (literal instanceof String) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a STRING column requires a floating "
                                        + "literal, i.e., Float or Double.");
                    }
                case BOOLEAN:
                    if (literal instanceof Boolean) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a BOOLEAN column requires a Boolean literal.");
                    }
                case DATE:
                    if (literal instanceof Date) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a DATE column requires a java.sql.Date literal.");
                    }
                case TIMESTAMP:
                    if (literal instanceof Timestamp) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a TIMESTAMP column requires a java.sql.Timestamp literal.");
                    }
                case DECIMAL:
                    if (literal instanceof BigDecimal) {
                        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) literal));
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a DECIMAL column requires a BigDecimal literal.");
                    }
                default:
                    throw new IllegalArgumentException("Unknown literal type " + literalType);
            }
        }
    }

    abstract static class BinaryPredicate extends ColumnPredicate {
        final Serializable literal;

        BinaryPredicate(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType);
            this.literal = literal;
        }
    }

    /** An EQUALS predicate that can be evaluated by the OrcInputFormat. */
    public static class Equals extends BinaryPredicate {
        /**
         * Creates an EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public Equals(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.equals(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " = " + literal;
        }
    }

    /** An EQUALS predicate that can be evaluated with Null safety by the OrcInputFormat. */
    public static class NullSafeEquals extends BinaryPredicate {
        /**
         * Creates a null-safe EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public NullSafeEquals(
                String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.nullSafeEquals(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " = " + literal;
        }
    }

    /** A LESS_THAN predicate that can be evaluated by the OrcInputFormat. */
    public static class LessThan extends BinaryPredicate {
        /**
         * Creates a LESS_THAN predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public LessThan(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.lessThan(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " < " + literal;
        }
    }

    /** A LESS_THAN_EQUALS predicate that can be evaluated by the OrcInputFormat. */
    public static class LessThanEquals extends BinaryPredicate {
        /**
         * Creates a LESS_THAN_EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public LessThanEquals(
                String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.lessThanEquals(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " <= " + literal;
        }
    }

    /** An IS_NULL predicate that can be evaluated by the OrcInputFormat. */
    public static class IsNull extends ColumnPredicate {
        /**
         * Creates an IS_NULL predicate.
         *
         * @param columnName The column to check for null.
         * @param literalType The type of the column to check for null.
         */
        public IsNull(String columnName, PredicateLeaf.Type literalType) {
            super(columnName, literalType);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.isNull(columnName, literalType);
        }

        @Override
        public String toString() {
            return columnName + " IS NULL";
        }
    }

    /** An BETWEEN predicate that can be evaluated by the OrcInputFormat. */
    public static class Between extends ColumnPredicate {
        private Serializable lowerBound;
        private Serializable upperBound;

        /**
         * Creates an BETWEEN predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literals.
         * @param lowerBound The literal value of the (inclusive) lower bound to check the column
         *     against.
         * @param upperBound The literal value of the (inclusive) upper bound to check the column
         *     against.
         */
        public Between(
                String columnName,
                PredicateLeaf.Type literalType,
                Serializable lowerBound,
                Serializable upperBound) {
            super(columnName, literalType);
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.between(
                    columnName, literalType, castLiteral(lowerBound), castLiteral(upperBound));
        }

        @Override
        public String toString() {
            return lowerBound + " <= " + columnName + " <= " + upperBound;
        }
    }

    /** An IN predicate that can be evaluated by the OrcInputFormat. */
    public static class In extends ColumnPredicate {
        private Serializable[] literals;

        /**
         * Creates an IN predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literals.
         * @param literals The literal values to check the column against.
         */
        public In(String columnName, PredicateLeaf.Type literalType, Serializable... literals) {
            super(columnName, literalType);
            this.literals = literals;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            Object[] castedLiterals = new Object[literals.length];
            for (int i = 0; i < literals.length; i++) {
                castedLiterals[i] = castLiteral(literals[i]);
            }
            return builder.in(columnName, literalType, (Object[]) castedLiterals);
        }

        @Override
        public String toString() {
            return columnName + " IN " + Arrays.toString(literals);
        }
    }

    /** A NOT predicate to negate a predicate that can be evaluated by the OrcInputFormat. */
    public static class Not extends Predicate {
        private final Predicate pred;

        /**
         * Creates a NOT predicate.
         *
         * @param predicate The predicate to negate.
         */
        public Not(Predicate predicate) {
            this.pred = predicate;
        }

        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return pred.add(builder.startNot()).end();
        }

        protected Predicate child() {
            return pred;
        }

        @Override
        public String toString() {
            return "NOT(" + pred.toString() + ")";
        }
    }

    /** An OR predicate that can be evaluated by the OrcInputFormat. */
    public static class Or extends Predicate {
        private final Predicate[] preds;

        /**
         * Creates an OR predicate.
         *
         * @param predicates The disjunctive predicates.
         */
        public Or(Predicate... predicates) {
            this.preds = predicates;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            SearchArgument.Builder withOr = builder.startOr();
            for (Predicate p : preds) {
                withOr = p.add(withOr);
            }
            return withOr.end();
        }

        protected Iterable<Predicate> children() {
            return Arrays.asList(preds);
        }

        @Override
        public String toString() {
            return "OR(" + Arrays.toString(preds) + ")";
        }
    }

    /** An AND predicate that can be evaluated by the OrcInputFormat. */
    public static class And extends Predicate {
        private final Predicate[] preds;

        /**
         * Creates an AND predicate.
         *
         * @param predicates The conjunctive predicates.
         */
        public And(Predicate... predicates) {
            this.preds = predicates;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            SearchArgument.Builder withAnd = builder.startAnd();
            for (Predicate pred : preds) {
                withAnd = pred.add(withAnd);
            }
            return withAnd.end();
        }

        @Override
        public String toString() {
            return "AND(" + Arrays.toString(preds) + ")";
        }
    }
}
