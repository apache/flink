/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.util;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.AggregateLambdaFactory;
import org.apache.calcite.adapter.enumerable.BasicAggregateLambdaFactory;
import org.apache.calcite.adapter.enumerable.BasicLazyAccumulator;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.LazyAggregateLambdaFactory;
import org.apache.calcite.adapter.enumerable.MatchUtils;
import org.apache.calcite.adapter.enumerable.SourceSorter;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.ExtendedEnumerable;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.MemoryFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnsignedType;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.metadata.BuiltInMetadata.AllPredicates;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Collation;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnUniqueness;
import org.apache.calcite.rel.metadata.BuiltInMetadata.CumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.DistinctRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Distribution;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ExplainVisibility;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ExpressionLineage;
import org.apache.calcite.rel.metadata.BuiltInMetadata.FunctionalDependency;
import org.apache.calcite.rel.metadata.BuiltInMetadata.LowerBoundCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.MaxRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Measure;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Memory;
import org.apache.calcite.rel.metadata.BuiltInMetadata.MinRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NodeTypes;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PercentageOriginalRows;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PopulationSize;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Predicates;
import org.apache.calcite.rel.metadata.BuiltInMetadata.RowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Selectivity;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Size;
import org.apache.calcite.rel.metadata.BuiltInMetadata.TableReferences;
import org.apache.calcite.rel.metadata.BuiltInMetadata.UniqueKeys;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Automaton;
import org.apache.calcite.runtime.BinarySearch;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.CompressionFunctions;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.FunctionContexts;
import org.apache.calcite.runtime.JsonFunctions;
import org.apache.calcite.runtime.Matcher;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.runtime.Pattern;
import org.apache.calcite.runtime.RandomFunction;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.runtime.SortedMultiMap;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.SqlFunctions.FlatProductInputType;
import org.apache.calcite.runtime.UrlFunctions;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.runtime.XmlFunctions;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation;
import org.apache.calcite.runtime.variant.VariantNull;
import org.apache.calcite.runtime.variant.VariantSqlValue;
import org.apache.calcite.runtime.variant.VariantValue;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonExistsErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Built-in methods. The whole class was copied from Calcite because of CALCITE-7716. Should be
 * removed after upgrade to 1.43.0
 */
public enum BuiltInMethod {
    QUERYABLE_SELECT(Queryable.class, "select", FunctionExpression.class),
    QUERYABLE_AS_ENUMERABLE(Queryable.class, "asEnumerable"),
    QUERYABLE_TABLE_AS_QUERYABLE(
            QueryableTable.class,
            "asQueryable",
            QueryProvider.class,
            SchemaPlus.class,
            String.class),
    AS_QUERYABLE(Enumerable.class, "asQueryable"),
    ABSTRACT_ENUMERABLE_CTOR(AbstractEnumerable.class),
    CHAR_DECIMAL_CAST(Primitive.class, "charToDecimalCast", String.class, int.class, int.class),
    CHAR_DECIMAL_CAST_ROUNDING_MODE(
            Primitive.class,
            "charToDecimalCast",
            String.class,
            int.class,
            int.class,
            RoundingMode.class),
    SHORT_INTERVAL_DECIMAL_CAST(
            Primitive.class,
            "shortIntervalToDecimalCast",
            Long.class,
            int.class,
            int.class,
            BigDecimal.class),
    SHORT_INTERVAL_DECIMAL_CAST_ROUNDING_MODE(
            Primitive.class,
            "shortIntervalToDecimalCast",
            Long.class,
            int.class,
            int.class,
            BigDecimal.class,
            RoundingMode.class),
    LONG_INTERVAL_DECIMAL_CAST(
            Primitive.class,
            "longIntervalToDecimalCast",
            Integer.class,
            int.class,
            int.class,
            BigDecimal.class),
    LONG_INTERVAL_DECIMAL_CAST_ROUNDING_MODE(
            Primitive.class,
            "longIntervalToDecimalCast",
            Integer.class,
            int.class,
            int.class,
            BigDecimal.class,
            RoundingMode.class),
    INTO(ExtendedEnumerable.class, "into", Collection.class),
    REMOVE_ALL(ExtendedEnumerable.class, "removeAll", Collection.class),
    SCHEMA_GET_SUB_SCHEMA(Schema.class, "getSubSchema", String.class),
    SCHEMA_GET_TABLE(Schema.class, "getTable", String.class),
    SCHEMA_PLUS_ADD_TABLE(SchemaPlus.class, "add", String.class, Table.class),
    SCHEMA_PLUS_REMOVE_TABLE(SchemaPlus.class, "removeTable", String.class),
    SCHEMA_PLUS_UNWRAP(SchemaPlus.class, "unwrap", Class.class),
    SCHEMAS_ENUMERABLE_SCANNABLE(
            Schemas.class, "enumerable", ScannableTable.class, DataContext.class),
    SCHEMAS_ENUMERABLE_FILTERABLE(
            Schemas.class, "enumerable", FilterableTable.class, DataContext.class),
    SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE(
            Schemas.class, "enumerable", ProjectableFilterableTable.class, DataContext.class),
    SCHEMAS_QUERYABLE(
            Schemas.class,
            "queryable",
            DataContext.class,
            SchemaPlus.class,
            Class.class,
            String.class),
    REFLECTIVE_SCHEMA_GET_TARGET(ReflectiveSchema.class, "getTarget"),
    DATA_CONTEXT_GET(DataContext.class, "get", String.class),
    DATA_CONTEXT_GET_ROOT_SCHEMA(DataContext.class, "getRootSchema"),
    JDBC_SCHEMA_DATA_SOURCE(JdbcSchema.class, "getDataSource"),
    ROW_VALUE(Row.class, "getObject", int.class),
    ROW_AS_COPY(Row.class, "asCopy", Object[].class),
    ROW_COPY_VALUES(
            Row.class, "copyValues"), // This is an instance method that returns an Object[].
    RESULT_SET_ENUMERABLE_SET_TIMEOUT(ResultSetEnumerable.class, "setTimeout", DataContext.class),
    RESULT_SET_ENUMERABLE_OF(
            ResultSetEnumerable.class, "of", DataSource.class, String.class, Function1.class),
    RESULT_SET_ENUMERABLE_OF_PREPARED(
            ResultSetEnumerable.class,
            "of",
            DataSource.class,
            String.class,
            Function1.class,
            ResultSetEnumerable.PreparedStatementEnricher.class),
    CREATE_ENRICHER(
            ResultSetEnumerable.class, "createEnricher", Integer[].class, DataContext.class),
    HASH_JOIN(
            ExtendedEnumerable.class,
            "hashJoin",
            Enumerable.class,
            Function1.class,
            Function1.class,
            Function2.class,
            EqualityComparer.class,
            boolean.class,
            boolean.class,
            Predicate2.class),
    ASOF_JOIN(
            ExtendedEnumerable.class,
            "asofJoin",
            Enumerable.class,
            Function1.class, // outer key selector
            Function1.class, // inner key selector
            Function2.class, // result selector
            Predicate2.class, // timestamp comparator
            Comparator.class, // match comparator
            boolean.class), // generateNullsOnRight
    MATCH(
            Enumerables.class,
            "match",
            Enumerable.class,
            Function1.class,
            Matcher.class,
            Enumerables.Emitter.class,
            int.class,
            int.class),
    PATTERN_BUILDER(Utilities.class, "patternBuilder"),
    PATTERN_BUILDER_SYMBOL(Pattern.PatternBuilder.class, "symbol", String.class),
    PATTERN_BUILDER_SEQ(Pattern.PatternBuilder.class, "seq"),
    PATTERN_BUILDER_BUILD(Pattern.PatternBuilder.class, "build"),
    PATTERN_TO_AUTOMATON(Pattern.PatternBuilder.class, "automaton"),
    MATCHER_BUILDER(Matcher.class, "builder", Automaton.class),
    MATCHER_BUILDER_ADD(Matcher.Builder.class, "add", String.class, Predicate.class),
    MATCHER_BUILDER_BUILD(Matcher.Builder.class, "build"),
    MATCH_UTILS_LAST_WITH_SYMBOL(
            MatchUtils.class, "lastWithSymbol", String.class, List.class, List.class, int.class),
    EMITTER_EMIT(
            Enumerables.Emitter.class,
            "emit",
            List.class,
            List.class,
            List.class,
            int.class,
            Consumer.class),
    MERGE_JOIN(
            EnumerableDefaults.class,
            "mergeJoin",
            Enumerable.class,
            Enumerable.class,
            Function1.class,
            Function1.class,
            Predicate2.class,
            Function2.class,
            JoinType.class,
            Comparator.class,
            EqualityComparer.class),
    SLICE0(Enumerables.class, "slice0", Enumerable.class),
    SEMI_JOIN(
            EnumerableDefaults.class,
            "semiJoin",
            Enumerable.class,
            Enumerable.class,
            Function1.class,
            Function1.class,
            EqualityComparer.class,
            Predicate2.class),
    ANTI_JOIN(
            EnumerableDefaults.class,
            "antiJoin",
            Enumerable.class,
            Enumerable.class,
            Function1.class,
            Function1.class,
            EqualityComparer.class,
            Predicate2.class),
    NESTED_LOOP_JOIN(
            EnumerableDefaults.class,
            "nestedLoopJoin",
            Enumerable.class,
            Enumerable.class,
            Predicate2.class,
            Function2.class,
            JoinType.class),
    CORRELATE_JOIN(
            ExtendedEnumerable.class,
            "correlateJoin",
            JoinType.class,
            Function1.class,
            Function2.class),
    CORRELATE_BATCH_JOIN(
            EnumerableDefaults.class,
            "correlateBatchJoin",
            JoinType.class,
            Enumerable.class,
            Function1.class,
            Function2.class,
            Predicate2.class,
            int.class),
    SELECT(ExtendedEnumerable.class, "select", Function1.class),
    SELECT2(ExtendedEnumerable.class, "select", Function2.class),
    SELECT_MANY(ExtendedEnumerable.class, "selectMany", Function1.class),
    WHERE(ExtendedEnumerable.class, "where", Predicate1.class),
    WHERE2(ExtendedEnumerable.class, "where", Predicate2.class),
    DISTINCT(ExtendedEnumerable.class, "distinct"),
    DISTINCT2(ExtendedEnumerable.class, "distinct", EqualityComparer.class),
    SORTED_GROUP_BY(
            ExtendedEnumerable.class,
            "sortedGroupBy",
            Function1.class,
            Function0.class,
            Function2.class,
            Function2.class,
            Comparator.class),
    GROUP_BY(ExtendedEnumerable.class, "groupBy", Function1.class),
    GROUP_BY2(
            ExtendedEnumerable.class,
            "groupBy",
            Function1.class,
            Function0.class,
            Function2.class,
            Function2.class),
    GROUP_BY_MULTIPLE(
            EnumerableDefaults.class,
            "groupByMultiple",
            Enumerable.class,
            List.class,
            Function0.class,
            Function2.class,
            Function2.class),
    AGGREGATE(
            ExtendedEnumerable.class, "aggregate", Object.class, Function2.class, Function1.class),
    ORDER_BY(ExtendedEnumerable.class, "orderBy", Function1.class, Comparator.class),
    ORDER_BY_WITH_FETCH_AND_OFFSET(
            EnumerableDefaults.class,
            "orderBy",
            Enumerable.class,
            Function1.class,
            Comparator.class,
            int.class,
            int.class),
    UNION(ExtendedEnumerable.class, "union", Enumerable.class),
    CONCAT(ExtendedEnumerable.class, "concat", Enumerable.class),
    REPEAT_UNION(
            EnumerableDefaults.class,
            "repeatUnion",
            Enumerable.class,
            Enumerable.class,
            int.class,
            boolean.class,
            EqualityComparer.class,
            Function0.class),
    MERGE_UNION(
            EnumerableDefaults.class,
            "mergeUnion",
            List.class,
            Function1.class,
            Comparator.class,
            boolean.class,
            EqualityComparer.class),
    LAZY_COLLECTION_SPOOL(
            EnumerableDefaults.class, "lazyCollectionSpool", Collection.class, Enumerable.class),
    INTERSECT(ExtendedEnumerable.class, "intersect", Enumerable.class, boolean.class),
    EXCEPT(ExtendedEnumerable.class, "except", Enumerable.class, boolean.class),
    SKIP(ExtendedEnumerable.class, "skip", int.class),
    TAKE(ExtendedEnumerable.class, "take", int.class),
    SINGLETON_ENUMERABLE(Linq4j.class, "singletonEnumerable", Object.class),
    EMPTY_ENUMERABLE(Linq4j.class, "emptyEnumerable"),
    NULLS_COMPARATOR(Functions.class, "nullsComparator", boolean.class, boolean.class),
    NULLS_COMPARATOR2(
            Functions.class, "nullsComparator", boolean.class, boolean.class, Comparator.class),
    ARRAY_COMPARER(Functions.class, "arrayComparer"),
    FUNCTION0_APPLY(Function0.class, "apply"),
    FUNCTION1_APPLY(Function1.class, "apply", Object.class),
    ARRAYS_AS_LIST(Arrays.class, "asList", Object[].class),
    ARRAY(SqlFunctions.class, "array", Object[].class),
    ARRAY_COPY(
            System.class, "arraycopy", Object.class, int.class, Object.class, int.class, int.class),
    // class PairList.Helper is deprecated to discourage code from calling its
    // methods directly, but use via Janino code generation is just fine.
    @SuppressWarnings("deprecation")
    PAIR_LIST_COPY_OF(PairList.Helper.class, "copyOf", Object.class, Object.class, Object[].class),
    FLAT_PRODUCT(
            SqlFunctions.class,
            "flatProduct",
            int[].class,
            boolean.class,
            FlatProductInputType[].class),
    FLAT_LIST(SqlFunctions.class, "flatList"),
    LIST_N(FlatLists.class, "copyOf", Comparable[].class),
    LIST2(FlatLists.class, "of", Object.class, Object.class),
    LIST3(FlatLists.class, "of", Object.class, Object.class, Object.class),
    LIST4(FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class),
    LIST5(
            FlatLists.class,
            "of",
            Object.class,
            Object.class,
            Object.class,
            Object.class,
            Object.class),
    LIST6(
            FlatLists.class,
            "of",
            Object.class,
            Object.class,
            Object.class,
            Object.class,
            Object.class,
            Object.class),
    COMPARABLE_EMPTY_LIST(FlatLists.class, "COMPARABLE_EMPTY_LIST", true),
    IDENTITY_COMPARER(Functions.class, "identityComparer"),
    IDENTITY_SELECTOR(Functions.class, "identitySelector"),
    AS_ENUMERABLE(Linq4j.class, "asEnumerable", Object[].class),
    AS_ENUMERABLE2(Linq4j.class, "asEnumerable", Iterable.class),
    ENUMERABLE_TO_LIST(ExtendedEnumerable.class, "toList"),
    ENUMERABLE_TO_MAP(ExtendedEnumerable.class, "toMap", Function1.class, Function1.class),
    AS_LIST(Primitive.class, "asList", Object.class),
    DECIMAL_DECIMAL_CAST(
            Primitive.class, "decimalDecimalCast", BigDecimal.class, int.class, int.class),
    DECIMAL_DECIMAL_CAST_ROUNDING_MODE(
            Primitive.class,
            "decimalDecimalCast",
            BigDecimal.class,
            int.class,
            int.class,
            RoundingMode.class),
    INTEGER_DECIMAL_CAST(Primitive.class, "integerDecimalCast", Number.class, int.class, int.class),
    INTEGER_DECIMAL_CAST_ROUNDING_MODE(
            Primitive.class,
            "integerDecimalCast",
            Number.class,
            int.class,
            int.class,
            RoundingMode.class),
    FP_DECIMAL_CAST(Primitive.class, "fpDecimalCast", Number.class, int.class, int.class),
    FP_DECIMAL_CAST_ROUNDING_MODE(
            Primitive.class,
            "fpDecimalCast",
            Number.class,
            int.class,
            int.class,
            RoundingMode.class),
    INTEGER_CAST(Primitive.class, "integerCast", Primitive.class, Object.class),
    INTEGER_CAST_ROUNDING_MODE(
            Primitive.class, "integerCast", Primitive.class, Object.class, RoundingMode.class),
    CAST_TO_UBYTE(UnsignedType.class, "toUByte", Number.class, RoundingMode.class),
    CAST_TO_USHORT(UnsignedType.class, "toUShort", Number.class, RoundingMode.class),
    CAST_TO_UINTEGER(UnsignedType.class, "toUInteger", Number.class, RoundingMode.class),
    CAST_TO_ULONG(UnsignedType.class, "toULong", Number.class, RoundingMode.class),
    MEMORY_GET0(MemoryFactory.Memory.class, "get"),
    MEMORY_GET1(MemoryFactory.Memory.class, "get", int.class),
    ENUMERATOR_CURRENT(Enumerator.class, "current"),
    ENUMERATOR_MOVE_NEXT(Enumerator.class, "moveNext"),
    ENUMERATOR_CLOSE(Enumerator.class, "close"),
    ENUMERATOR_RESET(Enumerator.class, "reset"),
    ENUMERABLE_ENUMERATOR(Enumerable.class, "enumerator"),
    ENUMERABLE_FOREACH(Enumerable.class, "foreach", Function1.class),
    ITERABLE_FOR_EACH(Iterable.class, "forEach", Consumer.class),
    FUNCTION_APPLY(Function.class, "apply", Object.class),
    PREDICATE_TEST(Predicate.class, "test", Object.class),
    BI_PREDICATE_TEST(BiPredicate.class, "test", Object.class, Object.class),
    CONSUMER_ACCEPT(Consumer.class, "accept", Object.class),
    TYPED_GET_ELEMENT_TYPE(ArrayBindable.class, "getElementType"),
    BINDABLE_BIND(Bindable.class, "bind", DataContext.class),
    RESULT_SET_GET_DATE2(ResultSet.class, "getDate", int.class, Calendar.class),
    RESULT_SET_GET_TIME2(ResultSet.class, "getTime", int.class, Calendar.class),
    RESULT_SET_GET_TIMESTAMP2(ResultSet.class, "getTimestamp", int.class, Calendar.class),
    TIME_ZONE_GET_OFFSET(TimeZone.class, "getOffset", long.class),
    LONG_VALUE(Number.class, "longValue"),
    STRING_TO_UPPER(String.class, "toUpperCase"),
    COMPARATOR_COMPARE(Comparator.class, "compare", Object.class, Object.class),
    COLLECTIONS_REVERSE_ORDER(Collections.class, "reverseOrder"),
    COLLECTIONS_EMPTY_LIST(Collections.class, "emptyList"),
    COLLECTIONS_SINGLETON_LIST(Collections.class, "singletonList", Object.class),
    COLLECTION_SIZE(Collection.class, "size"),
    COLLECTION_EXISTS(Functions.class, "exists", List.class, Predicate1.class),
    COLLECTION_ALL(Functions.class, "all", List.class, Predicate1.class),
    COLLECTION_NULLABLE_EXISTS(SqlFunctions.class, "nullableExists", List.class, Function1.class),
    COLLECTION_NULLABLE_ALL(SqlFunctions.class, "nullableAll", List.class, Function1.class),
    MAP_CLEAR(Map.class, "clear"),
    MAP_GET(Map.class, "get", Object.class),
    MAP_GET_OR_DEFAULT(Map.class, "getOrDefault", Object.class, Object.class),
    MAP_PUT(Map.class, "put", Object.class, Object.class),
    COLLECTION_ADD(Collection.class, "add", Object.class),
    COLLECTION_ADDALL(Collection.class, "addAll", Collection.class),
    COLLECTION_RETAIN_ALL(Collection.class, "retainAll", Collection.class),
    LIST_CONTAINS(List.class, "contains", Object.class),
    LIST_GET(List.class, "get", int.class),
    LIST_TO_ARRAY(List.class, "toArray"),
    LIST_TRANSFORM(SqlFunctions.class, "transform", List.class, Function1.class),
    ITERATOR_HAS_NEXT(Iterator.class, "hasNext"),
    ITERATOR_NEXT(Iterator.class, "next"),
    MATH_MAX(Math.class, "max", int.class, int.class),
    MATH_MIN(Math.class, "min", int.class, int.class),
    SORTED_MULTI_MAP_PUT_MULTI(SortedMultiMap.class, "putMulti", Object.class, Object.class),
    SORTED_MULTI_MAP_ARRAYS(SortedMultiMap.class, "arrays", Comparator.class),
    SORTED_MULTI_MAP_SINGLETON(
            SortedMultiMap.class, "singletonArrayIterator", Comparator.class, List.class),
    IMMUTABLE_INT_LIST_IDENTITY(ImmutableIntList.class, "identity", int.class),
    IMMUTABLE_INT_LIST_COPY_OF(ImmutableIntList.class, "copyOf", int[].class),
    BINARY_SEARCH5_LOWER(
            BinarySearch.class,
            "lowerBound",
            Object[].class,
            Object.class,
            int.class,
            int.class,
            Comparator.class),
    BINARY_SEARCH5_UPPER(
            BinarySearch.class,
            "upperBound",
            Object[].class,
            Object.class,
            int.class,
            int.class,
            Comparator.class),
    BINARY_SEARCH6_LOWER(
            BinarySearch.class,
            "lowerBound",
            Object[].class,
            Object.class,
            int.class,
            int.class,
            Function1.class,
            Comparator.class),
    BINARY_SEARCH6_UPPER(
            BinarySearch.class,
            "upperBound",
            Object[].class,
            Object.class,
            int.class,
            int.class,
            Function1.class,
            Comparator.class),
    ARRAY_ITEM(
            SqlFunctions.class,
            "arrayItemOptional",
            List.class,
            int.class,
            int.class,
            boolean.class),
    MAP_ITEM(SqlFunctions.class, "mapItemOptional", Map.class, Object.class),
    ANY_ITEM(SqlFunctions.class, "itemOptional", Object.class, Object.class),
    UPPER(SqlFunctions.class, "upper", String.class),
    LOWER(SqlFunctions.class, "lower", String.class),
    ARRAY_TO_STRING(SqlFunctions.class, "arrayToString", List.class, String.class),
    STRING_TO_ARRAY(SqlFunctions.class, "stringToArray", String.class, String.class, String.class),
    SROUND(SqlFunctions.class, "sround", long.class),
    STRUNCATE(SqlFunctions.class, "struncate", long.class),
    ASCII(SqlFunctions.class, "ascii", String.class),
    CHAR_FROM_ASCII(SqlFunctions.class, "charFromAscii", int.class),
    CHAR_FROM_UTF8(SqlFunctions.class, "charFromUtf8", int.class),
    CODE_POINTS_TO_BYTES(SqlFunctions.class, "codePointsToBytes", List.class),
    CODE_POINTS_TO_STRING(SqlFunctions.class, "codePointsToString", List.class),
    TO_CODE_POINTS(SqlFunctions.class, "toCodePoints", String.class),
    CONVERT(SqlFunctions.class, "convertWithCharset", String.class, String.class, String.class),
    CONVERT_ORACLE(SqlFunctions.class, "convertOracle", String.class, String[].class),
    EXP(SqlFunctions.class, "exp", double.class),
    MOD(SqlFunctions.class, "mod", long.class, long.class),
    POWER(SqlFunctions.class, "power", double.class, double.class),
    POWER_PG(SqlFunctions.class, "power", BigDecimal.class, BigDecimal.class),
    REPEAT(SqlFunctions.class, "repeat", String.class, int.class),
    SPACE(SqlFunctions.class, "space", int.class),
    SPLIT(SqlFunctions.class, "split", String.class),
    SPLIT_PART(SqlFunctions.class, "splitPart", String.class, String.class, int.class),
    SOUNDEX(SqlFunctions.class, "soundex", String.class),
    SOUNDEX_SPARK(SqlFunctions.class, "soundexSpark", String.class),
    STRCMP(SqlFunctions.class, "strcmp", String.class, String.class),
    DIFFERENCE(SqlFunctions.class, "difference", String.class, String.class),
    REVERSE(SqlFunctions.class, "reverse", String.class),
    LEVENSHTEIN(SqlFunctions.class, "levenshtein", String.class, String.class),
    FIND_IN_SET(SqlFunctions.class, "findInSet", String.class, String.class),
    LEFT(SqlFunctions.class, "left", String.class, int.class),
    RIGHT(SqlFunctions.class, "right", String.class, int.class),
    TO_BASE64(SqlFunctions.class, "toBase64", String.class),
    FROM_BASE64(SqlFunctions.class, "fromBase64", String.class),
    TO_BASE32(SqlFunctions.class, "toBase32", String.class),
    FROM_BASE32(SqlFunctions.class, "fromBase32", String.class),
    HEX(SqlFunctions.class, "hex", String.class),
    TO_HEX(SqlFunctions.class, "toHex", ByteString.class),
    FROM_HEX(SqlFunctions.class, "fromHex", String.class),
    BIN(SqlFunctions.class, "bin", long.class),
    MD5(SqlFunctions.class, "md5", String.class),
    CRC32(SqlFunctions.class, "crc32", String.class),
    SHA1(SqlFunctions.class, "sha1", String.class),
    SHA256(SqlFunctions.class, "sha256", String.class),
    SHA512(SqlFunctions.class, "sha512", String.class),
    THROW_UNLESS(SqlFunctions.class, "throwUnless", boolean.class, String.class),
    COMPRESS(CompressionFunctions.class, "compress", String.class),
    URL_DECODE(UrlFunctions.class, "urlDecode", String.class),
    URL_ENCODE(UrlFunctions.class, "urlEncode", String.class),
    EXTRACT_VALUE(XmlFunctions.class, "extractValue", String.class, String.class),
    XML_TRANSFORM(XmlFunctions.class, "xmlTransform", String.class, String.class),
    EXTRACT_XML(XmlFunctions.class, "extractXml", String.class, String.class, String.class),
    EXISTS_NODE(XmlFunctions.class, "existsNode", String.class, String.class, String.class),
    JSONIZE(JsonFunctions.class, "jsonize", Object.class),
    DEJSONIZE(JsonFunctions.class, "dejsonize", String.class),
    JSON_VALUE_EXPRESSION(JsonFunctions.class, "jsonValueExpression", String.class),
    JSON_API_COMMON_SYNTAX(JsonFunctions.class, "jsonApiCommonSyntax", String.class, String.class),
    JSON_API_COMMON_SYNTAX_WITH_CACHE(
            JsonFunctions.StatefulFunction.class,
            "jsonApiCommonSyntaxWithCache",
            String.class,
            String.class),
    JSON_EXISTS2(JsonFunctions.StatefulFunction.class, "jsonExists", String.class, String.class),
    JSON_EXISTS3(
            JsonFunctions.StatefulFunction.class,
            "jsonExists",
            String.class,
            String.class,
            SqlJsonExistsErrorBehavior.class),
    JSON_VALUE(
            JsonFunctions.StatefulFunction.class,
            "jsonValue",
            String.class,
            String.class,
            SqlJsonValueEmptyOrErrorBehavior.class,
            Object.class,
            SqlJsonValueEmptyOrErrorBehavior.class,
            Object.class),
    JSON_QUERY(
            JsonFunctions.StatefulFunction.class,
            "jsonQuery",
            String.class,
            String.class,
            SqlJsonQueryWrapperBehavior.class,
            SqlJsonQueryEmptyOrErrorBehavior.class,
            SqlJsonQueryEmptyOrErrorBehavior.class,
            boolean.class),
    JSON_OBJECT(JsonFunctions.class, "jsonObject", SqlJsonConstructorNullClause.class),
    JSON_TYPE(JsonFunctions.class, "jsonType", String.class),
    JSON_DEPTH(JsonFunctions.class, "jsonDepth", String.class),
    JSON_KEYS(JsonFunctions.class, "jsonKeys", String.class),
    JSON_INSERT(JsonFunctions.class, "jsonInsert", String.class, Object.class),
    JSON_PRETTY(JsonFunctions.class, "jsonPretty", String.class),
    JSON_LENGTH(JsonFunctions.class, "jsonLength", String.class),
    JSON_REPLACE(JsonFunctions.class, "jsonReplace", String.class, Object.class),
    JSON_REMOVE(JsonFunctions.class, "jsonRemove", String.class),
    JSON_STORAGE_SIZE(JsonFunctions.class, "jsonStorageSize", String.class),
    JSON_SET(JsonFunctions.class, "jsonSet", String.class, Object.class),
    JSON_OBJECTAGG_ADD(
            JsonFunctions.class,
            "jsonObjectAggAdd",
            Map.class,
            String.class,
            Object.class,
            SqlJsonConstructorNullClause.class),
    JSON_ARRAY(JsonFunctions.class, "jsonArray", SqlJsonConstructorNullClause.class),
    JSON_ARRAYAGG_ADD(
            JsonFunctions.class,
            "jsonArrayAggAdd",
            List.class,
            Object.class,
            SqlJsonConstructorNullClause.class),
    IS_JSON_VALUE(JsonFunctions.class, "isJsonValue", String.class),
    IS_JSON_OBJECT(JsonFunctions.class, "isJsonObject", String.class),
    IS_JSON_ARRAY(JsonFunctions.class, "isJsonArray", String.class),
    IS_JSON_SCALAR(JsonFunctions.class, "isJsonScalar", String.class),
    ST_GEOM_FROM_EWKT(SpatialTypeFunctions.class, "ST_GeomFromEWKT", String.class),
    UUID_FROM_STRING(UuidValue.class, "fromString", String.class),
    UUID_TO_STRING(SqlFunctions.class, "uuidToString", UuidValue.class),
    UUID_TO_BINARY(SqlFunctions.class, "uuidToBinary", UuidValue.class),
    BINARY_TO_UUID(SqlFunctions.class, "binaryToUuid", ByteString.class),
    INITCAP(SqlFunctions.class, "initcap", String.class),
    SUBSTRING(SqlFunctions.class, "substring", String.class, int.class, int.class),
    FORMAT_NUMBER(SqlFunctions.class, "formatNumber", long.class, int.class),
    LPAD(SqlFunctions.class, "lpad", String.class, int.class, String.class),
    RPAD(SqlFunctions.class, "rpad", String.class, int.class, String.class),
    STARTS_WITH(SqlFunctions.class, "startsWith", String.class, String.class),
    ENDS_WITH(SqlFunctions.class, "endsWith", String.class, String.class),
    OCTET_LENGTH(SqlFunctions.class, "octetLength", ByteString.class),
    CHAR_LENGTH(SqlFunctions.class, "charLength", String.class),
    BIT_LENGTH(SqlFunctions.class, "bitLength", String.class),
    BIT_GET(SqlFunctions.class, "bitGet", long.class, int.class),
    STRING_CONCAT(SqlFunctions.class, "concat", String.class, String.class),
    STRING_CONCAT_WITH_NULL(SqlFunctions.class, "concatWithNull", String.class, String.class),
    PARSE_URL2(SqlFunctions.ParseUrlFunction.class, "parseUrl", String.class, String.class),
    PARSE_URL3(
            SqlFunctions.ParseUrlFunction.class,
            "parseUrl",
            String.class,
            String.class,
            String.class),
    MULTI_STRING_CONCAT(SqlFunctions.class, "concatMulti", String[].class),
    MULTI_STRING_CONCAT_WITH_NULL(SqlFunctions.class, "concatMultiWithNull", String[].class),
    MULTI_STRING_CONCAT_WITH_SEPARATOR(
            SqlFunctions.class, "concatMultiWithSeparator", String[].class),
    MULTI_TYPE_STRING_ARRAY_CONCAT_WITH_SEPARATOR(
            SqlFunctions.class, "concatMultiTypeWithSeparator", String.class, Object[].class),
    MULTI_TYPE_OBJECT_CONCAT_WITH_SEPARATOR(
            SqlFunctions.class, "concatMultiObjectWithSeparator", String.class, Object[].class),
    FLOOR_DIV(Math.class, "floorDiv", long.class, long.class),
    FLOOR_MOD(Math.class, "floorMod", long.class, long.class),
    ADD_MONTHS(DateTimeUtils.class, "addMonths", long.class, int.class),
    ADD_MONTHS_INT(DateTimeUtils.class, "addMonths", int.class, int.class),
    SUBTRACT_MONTHS(DateTimeUtils.class, "subtractMonths", long.class, long.class),
    FLOOR(SqlFunctions.class, "floor", int.class, int.class),
    CEIL(SqlFunctions.class, "ceil", int.class, int.class),
    ABS(SqlFunctions.class, "abs", long.class),
    ACOS(SqlFunctions.class, "acos", double.class),
    ACOSD(SqlFunctions.class, "acosd", double.class),
    ACOSH(SqlFunctions.class, "acosh", double.class),
    ASIN(SqlFunctions.class, "asin", double.class),
    ASIND(SqlFunctions.class, "asind", double.class),
    ASINH(SqlFunctions.class, "asinh", double.class),
    ATAN(SqlFunctions.class, "atan", double.class),
    ATAN2(SqlFunctions.class, "atan2", double.class, double.class),
    ATAND(SqlFunctions.class, "atand", double.class),
    ATANH(SqlFunctions.class, "atanh", double.class),
    CBRT(SqlFunctions.class, "cbrt", double.class),
    COS(SqlFunctions.class, "cos", double.class),
    COSD(SqlFunctions.class, "cosd", double.class),
    COSH(SqlFunctions.class, "cosh", long.class),
    COT(SqlFunctions.class, "cot", double.class),
    COTH(SqlFunctions.class, "coth", double.class),
    CSC(SqlFunctions.class, "csc", double.class),
    CSCH(SqlFunctions.class, "csch", double.class),
    DEGREES(SqlFunctions.class, "degrees", double.class),
    FACTORIAL(SqlFunctions.class, "factorial", int.class),
    IS_INF(SqlFunctions.class, "isInf", long.class),
    IS_NAN(SqlFunctions.class, "isNaN", double.class),
    OVERLAY(SqlFunctions.class, "overlay", String.class, String.class, int.class),
    OVERLAY3(SqlFunctions.class, "overlay", String.class, String.class, int.class, int.class),
    POSITION(SqlFunctions.class, "position", String.class, String.class),
    RADIANS(SqlFunctions.class, "radians", double.class),
    RAND(RandomFunction.class, "rand"),
    RAND_SEED(RandomFunction.class, "randSeed", int.class),
    RAND_INTEGER(RandomFunction.class, "randInteger", int.class),
    RAND_INTEGER_SEED(RandomFunction.class, "randIntegerSeed", int.class, int.class),
    SAFE_ADD(SqlFunctions.class, "safeAdd", double.class, double.class),
    SAFE_DIVIDE(SqlFunctions.class, "safeDivide", double.class, double.class),
    SAFE_MULTIPLY(SqlFunctions.class, "safeMultiply", double.class, double.class),
    SAFE_SUBTRACT(SqlFunctions.class, "safeSubtract", double.class, double.class),
    LOG(SqlFunctions.class, "log", long.class, long.class, boolean.class),
    LOG1P(SqlFunctions.class, "log1p", long.class),
    SEC(SqlFunctions.class, "sec", double.class),
    SECH(SqlFunctions.class, "sech", double.class),
    SIGN(SqlFunctions.class, "sign", long.class),
    SIN(SqlFunctions.class, "sin", double.class),
    SIND(SqlFunctions.class, "sind", double.class),
    TAN(SqlFunctions.class, "tan", double.class),
    TAND(SqlFunctions.class, "tand", double.class),
    TANH(SqlFunctions.class, "tanh", long.class),
    SINH(SqlFunctions.class, "sinh", long.class),
    TRUNCATE(SqlFunctions.class, "truncate", String.class, int.class),
    TRUNCATE_OR_PAD(SqlFunctions.class, "truncateOrPad", String.class, int.class),
    TRIM(
            SqlFunctions.class,
            "trim",
            boolean.class,
            boolean.class,
            String.class,
            String.class,
            boolean.class),
    REPLACE(SqlFunctions.class, "replace", String.class, String.class, String.class, boolean.class),
    TRANSLATE_WITH_CHARSET(SqlFunctions.class, "translateWithCharset", String.class, String.class),
    TRANSLATE3(SqlFunctions.class, "translate3", String.class, String.class, String.class),
    LTRIM(SqlFunctions.class, "ltrim", String.class),
    RTRIM(SqlFunctions.class, "rtrim", String.class),
    LIKE(SqlFunctions.LikeFunction.class, "like", String.class, String.class),
    LIKE_ESCAPE(SqlFunctions.LikeFunction.class, "like", String.class, String.class, String.class),
    ILIKE(SqlFunctions.LikeFunction.class, "ilike", String.class, String.class),
    ILIKE_ESCAPE(
            SqlFunctions.LikeFunction.class, "ilike", String.class, String.class, String.class),
    RLIKE(SqlFunctions.RegexFunction.class, "rlike", String.class, String.class),
    SIMILAR(SqlFunctions.SimilarFunction.class, "similar", String.class, String.class),
    SIMILAR_ESCAPE(
            SqlFunctions.SimilarEscapeFunction.class,
            "similar",
            String.class,
            String.class,
            String.class),
    POSIX_REGEX_INSENSITIVE(
            SqlFunctions.PosixRegexFunction.class,
            "posixRegexInsensitive",
            String.class,
            String.class),
    POSIX_REGEX_SENSITIVE(
            SqlFunctions.PosixRegexFunction.class,
            "posixRegexSensitive",
            String.class,
            String.class),
    REGEXP_CONTAINS(SqlFunctions.RegexFunction.class, "regexpContains", String.class, String.class),
    REGEXP_EXTRACT2(SqlFunctions.RegexFunction.class, "regexpExtract", String.class, String.class),
    REGEXP_EXTRACT3(
            SqlFunctions.RegexFunction.class,
            "regexpExtract",
            String.class,
            String.class,
            int.class),
    REGEXP_EXTRACT4(
            SqlFunctions.RegexFunction.class,
            "regexpExtract",
            String.class,
            String.class,
            int.class,
            int.class),
    REGEXP_EXTRACT_ALL(
            SqlFunctions.RegexFunction.class, "regexpExtractAll", String.class, String.class),
    REGEXP_INSTR2(SqlFunctions.RegexFunction.class, "regexpInstr", String.class, String.class),
    REGEXP_INSTR3(
            SqlFunctions.RegexFunction.class, "regexpInstr", String.class, String.class, int.class),
    REGEXP_INSTR4(
            SqlFunctions.RegexFunction.class,
            "regexpInstr",
            String.class,
            String.class,
            int.class,
            int.class),
    REGEXP_INSTR5(
            SqlFunctions.RegexFunction.class,
            "regexpInstr",
            String.class,
            String.class,
            int.class,
            int.class,
            int.class),
    REGEXP_LIKE3(
            SqlFunctions.RegexFunction.class,
            "regexpLike",
            String.class,
            String.class,
            String.class),
    REGEXP_REPLACE2(SqlFunctions.RegexFunction.class, "regexpReplace", String.class, String.class),
    REGEXP_REPLACE3(
            SqlFunctions.RegexFunction.class,
            "regexpReplace",
            String.class,
            String.class,
            String.class),
    REGEXP_REPLACE4(
            SqlFunctions.RegexFunction.class,
            "regexpReplace",
            String.class,
            String.class,
            String.class,
            int.class),
    REGEXP_REPLACE5_OCCURRENCE(
            SqlFunctions.RegexFunction.class,
            "regexpReplace",
            String.class,
            String.class,
            String.class,
            int.class,
            int.class),
    REGEXP_REPLACE5_MATCHTYPE(
            SqlFunctions.RegexFunction.class,
            "regexpReplace",
            String.class,
            String.class,
            String.class,
            int.class,
            String.class),
    REGEXP_REPLACE6(
            SqlFunctions.RegexFunction.class,
            "regexpReplace",
            String.class,
            String.class,
            String.class,
            int.class,
            int.class,
            String.class),
    REGEXP_REPLACE_BIG_QUERY_3(
            SqlFunctions.RegexFunction.class,
            "regexpReplaceNonDollarIndexed",
            String.class,
            String.class,
            String.class),
    REGEXP_REPLACE_PG_3(
            SqlFunctions.RegexFunction.class,
            "regexpReplacePg",
            String.class,
            String.class,
            String.class),
    REGEXP_REPLACE_PG_4(
            SqlFunctions.RegexFunction.class,
            "regexpReplacePg",
            String.class,
            String.class,
            String.class,
            String.class),
    IS_TRUE(SqlFunctions.class, "isTrue", Boolean.class),
    IS_NOT_FALSE(SqlFunctions.class, "isNotFalse", Boolean.class),
    NOT(SqlFunctions.class, "not", Boolean.class),
    LESSER(SqlFunctions.class, "lesser", Comparable.class, Comparable.class),
    GREATER(SqlFunctions.class, "greater", Comparable.class, Comparable.class),
    LT_NULLABLE(SqlFunctions.class, "ltNullable", Comparable.class, Comparable.class),
    GT_NULLABLE(SqlFunctions.class, "gtNullable", Comparable.class, Comparable.class),
    LT(SqlFunctions.class, "lt", boolean.class, boolean.class),
    GT(SqlFunctions.class, "gt", boolean.class, boolean.class),
    BIT_AND(SqlFunctions.class, "bitAnd", long.class, long.class),
    BITCOUNT(SqlFunctions.class, "bitCount", BigDecimal.class),
    BIT_OR(SqlFunctions.class, "bitOr", long.class, long.class),
    BIT_XOR(SqlFunctions.class, "bitXor", long.class, long.class),
    BIT_NOT(SqlFunctions.class, "bitNot", long.class),
    LEFT_SHIFT(SqlFunctions.class, "leftShift", int.class, int.class),
    MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION(ModifiableTable.class, "getModifiableCollection"),
    SCANNABLE_TABLE_SCAN(ScannableTable.class, "scan", DataContext.class),
    STRING_TO_BOOLEAN(SqlFunctions.class, "toBoolean", String.class),
    INTERNAL_TO_DATE(SqlFunctions.class, "internalToDate", int.class),
    INTERNAL_TO_TIME(SqlFunctions.class, "internalToTime", int.class),
    INTERNAL_TO_TIMESTAMP(SqlFunctions.class, "internalToTimestamp", long.class),
    STRING_TO_DATE(DateTimeUtils.class, "dateStringToUnixDate", String.class),
    STRING_TO_TIME(DateTimeUtils.class, "timeStringToUnixDate", String.class),
    STRING_TO_TIMESTAMP(DateTimeUtils.class, "timestampStringToUnixDate", String.class),
    STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(
            SqlFunctions.class, "toTimeWithLocalTimeZone", String.class),
    TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(
            SqlFunctions.class, "toTimeWithLocalTimeZone", String.class, TimeZone.class),
    STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            SqlFunctions.class, "toTimestampWithLocalTimeZone", String.class),
    STRING_TO_BINARY(SqlFunctions.class, "stringToBinary", String.class, Charset.class),
    TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            SqlFunctions.class, "toTimestampWithLocalTimeZone", String.class, TimeZone.class),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIME(
            SqlFunctions.class, "timeWithLocalTimeZoneToTime", int.class, TimeZone.class),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(
            SqlFunctions.class,
            "timeWithLocalTimeZoneToTimestamp",
            String.class,
            int.class,
            TimeZone.class),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            SqlFunctions.class,
            "timeWithLocalTimeZoneToTimestampWithLocalTimeZone",
            String.class,
            int.class),
    TIME_WITH_LOCAL_TIME_ZONE_TO_STRING(
            SqlFunctions.class, "timeWithLocalTimeZoneToString", int.class, TimeZone.class),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE(
            SqlFunctions.class, "timestampWithLocalTimeZoneToDate", long.class, TimeZone.class),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME(
            SqlFunctions.class, "timestampWithLocalTimeZoneToTime", long.class, TimeZone.class),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE(
            SqlFunctions.class, "timestampWithLocalTimeZoneToTimeWithLocalTimeZone", long.class),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(
            SqlFunctions.class,
            "timestampWithLocalTimeZoneToTimestamp",
            long.class,
            TimeZone.class),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING(
            SqlFunctions.class, "timestampWithLocalTimeZoneToString", long.class, TimeZone.class),
    TIMESTAMP_SECONDS(SqlFunctions.class, "timestampSeconds", long.class),
    TIMESTAMP_MILLIS(SqlFunctions.class, "timestampMillis", long.class),
    TIMESTAMP_MICROS(SqlFunctions.class, "timestampMicros", long.class),
    UNIX_SECONDS(SqlFunctions.class, "unixSeconds", long.class),
    UNIX_MILLIS(SqlFunctions.class, "unixMillis", long.class),
    UNIX_MICROS(SqlFunctions.class, "unixMicros", long.class),
    DATE_FROM_UNIX_DATE(SqlFunctions.class, "dateFromUnixDate", int.class),
    UNIX_DATE(SqlFunctions.class, "unixDate", int.class),
    DATE(SqlFunctions.class, "date", int.class),
    DATETIME(SqlFunctions.class, "datetime", long.class),
    TIME(SqlFunctions.class, "time", int.class),
    TIMESTAMP(SqlFunctions.class, "timestamp", long.class),
    PARSE_DATE(SqlFunctions.DateParseFunction.class, "parseDate", String.class, String.class),
    PARSE_DATETIME(
            SqlFunctions.DateParseFunction.class, "parseDatetime", String.class, String.class),
    PARSE_TIME(SqlFunctions.DateParseFunction.class, "parseTime", String.class, String.class),
    PARSE_TIMESTAMP(
            SqlFunctions.DateParseFunction.class, "parseTimestamp", String.class, String.class),
    FORMAT_TIMESTAMP(
            SqlFunctions.DateFormatFunction.class, "formatTimestamp", String.class, long.class),
    TO_CHAR(SqlFunctions.DateFormatFunction.class, "toChar", long.class, String.class),
    TO_CHAR_PG(SqlFunctions.DateFormatFunctionPg.class, "toChar", long.class, String.class),
    TO_DATE(SqlFunctions.DateFormatFunction.class, "toDate", String.class, String.class),
    TO_DATE_PG(SqlFunctions.DateFormatFunctionPg.class, "toDate", String.class, String.class),
    TO_TIMESTAMP(SqlFunctions.DateFormatFunction.class, "toTimestamp", String.class, String.class),
    TO_TIMESTAMP_PG(
            SqlFunctions.DateFormatFunctionPg.class, "toTimestamp", String.class, String.class),
    FORMAT_DATE(SqlFunctions.DateFormatFunction.class, "formatDate", String.class, int.class),
    FORMAT_TIME(SqlFunctions.DateFormatFunction.class, "formatTime", String.class, int.class),
    UNIX_DATE_TO_STRING(DateTimeUtils.class, "unixDateToString", int.class),
    UNIX_TIME_TO_STRING(DateTimeUtils.class, "unixTimeToString", int.class),
    UNIX_TIMESTAMP_TO_STRING(DateTimeUtils.class, "unixTimestampToString", long.class),
    INTERVAL_YEAR_MONTH_TO_STRING(
            DateTimeUtils.class, "intervalYearMonthToString", int.class, TimeUnitRange.class),
    INTERVAL_DAY_TIME_TO_STRING(
            DateTimeUtils.class,
            "intervalDayTimeToString",
            long.class,
            TimeUnitRange.class,
            int.class),
    UNIX_DATE_EXTRACT(SqlFunctions.class, "unixDateExtract", TimeUnitRange.class, long.class),
    UNIX_DATE_FLOOR(DateTimeUtils.class, "unixDateFloor", TimeUnitRange.class, int.class),
    UNIX_DATE_CEIL(DateTimeUtils.class, "unixDateCeil", TimeUnitRange.class, int.class),
    UNIX_TIMESTAMP_FLOOR(
            DateTimeUtils.class, "unixTimestampFloor", TimeUnitRange.class, long.class),
    UNIX_TIMESTAMP_CEIL(DateTimeUtils.class, "unixTimestampCeil", TimeUnitRange.class, long.class),
    CUSTOM_DATE_ADD(
            SqlFunctions.class,
            "customDateAdd",
            DataContext.class,
            String.class,
            int.class,
            int.class),
    CUSTOM_DATE_DIFF(
            SqlFunctions.class,
            "customDateDiff",
            DataContext.class,
            String.class,
            int.class,
            int.class),
    CUSTOM_DATE_FLOOR(
            SqlFunctions.class, "customDateFloor", DataContext.class, String.class, int.class),
    CUSTOM_DATE_CEIL(
            SqlFunctions.class, "customDateCeil", DataContext.class, String.class, int.class),
    CUSTOM_TIMESTAMP_ADD(
            SqlFunctions.class,
            "customTimestampAdd",
            DataContext.class,
            String.class,
            long.class,
            long.class),
    CUSTOM_TIMESTAMP_DIFF(
            SqlFunctions.class,
            "customTimestampDiff",
            DataContext.class,
            String.class,
            long.class,
            long.class),
    CUSTOM_TIMESTAMP_FLOOR(
            SqlFunctions.class,
            "customTimestampFloor",
            DataContext.class,
            String.class,
            long.class),
    CUSTOM_TIMESTAMP_CEIL(
            SqlFunctions.class, "customTimestampCeil", DataContext.class, String.class, long.class),
    TIMESTAMP_TO_DATE(SqlFunctions.class, "timestampToDate", long.class),
    LAST_DAY(DateTimeUtils.class, "lastDay", int.class),
    DAYNAME_WITH_TIMESTAMP(SqlFunctions.class, "dayNameWithTimestamp", long.class, Locale.class),
    DAYNAME_WITH_DATE(SqlFunctions.class, "dayNameWithDate", int.class, Locale.class),
    MONTHNAME_WITH_TIMESTAMP(
            SqlFunctions.class, "monthNameWithTimestamp", long.class, Locale.class),
    MONTHNAME_WITH_DATE(SqlFunctions.class, "monthNameWithDate", int.class, Locale.class),
    CURRENT_TIMESTAMP(SqlFunctions.class, "currentTimestamp", DataContext.class),
    CURRENT_TIME(SqlFunctions.class, "currentTime", DataContext.class),
    CURRENT_DATE(SqlFunctions.class, "currentDate", DataContext.class),
    CURRENT_DATETIME(SqlFunctions.class, "currentDatetime", DataContext.class),
    CURRENT_DATETIME2(SqlFunctions.class, "currentDatetime", DataContext.class, String.class),
    LOCAL_TIMESTAMP(SqlFunctions.class, "localTimestamp", DataContext.class),
    LOCAL_TIME(SqlFunctions.class, "localTime", DataContext.class),
    SYSDATE(SqlFunctions.class, "sysDate", DataContext.class),
    SYSTIMESTAMP(SqlFunctions.class, "sysTimestamp", DataContext.class),
    TIME_ZONE(SqlFunctions.class, "timeZone", DataContext.class),
    USER(SqlFunctions.class, "user", DataContext.class),
    SYSTEM_USER(SqlFunctions.class, "systemUser", DataContext.class),
    LOCALE(SqlFunctions.class, "locale", DataContext.class),
    BOOLEAN_TO_STRING(SqlFunctions.class, "toString", boolean.class),
    JDBC_ARRAY_TO_LIST(SqlFunctions.class, "arrayToList", java.sql.Array.class),
    OBJECT_TO_STRING(Object.class, "toString"),
    OBJECTS_EQUAL(Objects.class, "equals", Object.class, Object.class),
    HASH(Utilities.class, "hash", int.class, Object.class),
    COMPARE(Utilities.class, "compare", Comparable.class, Comparable.class),
    COMPARE_NULLS_FIRST(Utilities.class, "compareNullsFirst", Comparable.class, Comparable.class),
    COMPARE_NULLS_LAST(Utilities.class, "compareNullsLast", Comparable.class, Comparable.class),
    COMPARE2(Utilities.class, "compare", Comparable.class, Comparable.class, Comparator.class),
    COMPARE_NULLS_FIRST2(
            Utilities.class,
            "compareNullsFirst",
            Comparable.class,
            Comparable.class,
            Comparator.class),
    COMPARE_NULLS_LAST2(
            Utilities.class,
            "compareNullsLast",
            Comparable.class,
            Comparable.class,
            Comparator.class),
    ROUND_LONG(SqlFunctions.class, "round", long.class, long.class),
    ROUND_INT(SqlFunctions.class, "round", int.class, int.class),
    DATE_TO_INT(SqlFunctions.class, "toInt", java.sql.Date.class),
    DATE_TO_INT_OFFSET(SqlFunctions.class, "toInt", java.sql.Date.class, TimeZone.class),
    DATE_TO_INT_OPTIONAL(SqlFunctions.class, "toIntOptional", java.sql.Date.class),
    DATE_TO_INT_OPTIONAL_OFFSET(
            SqlFunctions.class, "toIntOptional", java.sql.Date.class, TimeZone.class),
    TIME_TO_INT(SqlFunctions.class, "toInt", Time.class),
    TIME_TO_INT_OPTIONAL(SqlFunctions.class, "toIntOptional", Time.class),
    TIMESTAMP_TO_LONG(SqlFunctions.class, "toLong", Timestamp.class),
    TIMESTAMP_TO_LONG_OFFSET(SqlFunctions.class, "toLong", Timestamp.class, TimeZone.class),
    TIMESTAMP_TO_LONG_OPTIONAL(SqlFunctions.class, "toLongOptional", Timestamp.class),
    TIMESTAMP_TO_LONG_OPTIONAL_OFFSET(
            SqlFunctions.class, "toLongOptional", Timestamp.class, TimeZone.class),
    SEQUENCE_CURRENT_VALUE(SqlFunctions.class, "sequenceCurrentValue", String.class),
    SEQUENCE_NEXT_VALUE(SqlFunctions.class, "sequenceNextValue", String.class),
    SLICE(SqlFunctions.class, "slice", List.class),
    ELEMENT(SqlFunctions.class, "element", List.class),
    MEMBER_OF(SqlFunctions.class, "memberOf", Object.class, Collection.class),
    MULTISET_INTERSECT_DISTINCT(
            SqlFunctions.class, "multisetIntersectDistinct", Collection.class, Collection.class),
    MULTISET_INTERSECT_ALL(
            SqlFunctions.class, "multisetIntersectAll", Collection.class, Collection.class),
    MULTISET_EXCEPT_DISTINCT(
            SqlFunctions.class, "multisetExceptDistinct", Collection.class, Collection.class),
    MULTISET_EXCEPT_ALL(
            SqlFunctions.class, "multisetExceptAll", Collection.class, Collection.class),
    MULTISET_UNION_DISTINCT(
            SqlFunctions.class, "multisetUnionDistinct", Collection.class, Collection.class),
    MULTISET_UNION_ALL(SqlFunctions.class, "multisetUnionAll", Collection.class, Collection.class),
    IS_A_SET(SqlFunctions.class, "isASet", Collection.class),
    IS_EMPTY(Collection.class, "isEmpty"),
    SUBMULTISET_OF(SqlFunctions.class, "submultisetOf", Collection.class, Collection.class),
    ARRAY_APPEND(SqlFunctions.class, "arrayAppend", List.class, Object.class),
    ARRAY_COMPACT(SqlFunctions.class, "compact", List.class),
    ARRAY_DISTINCT(SqlFunctions.class, "distinct", List.class),
    ARRAY_MAX(SqlFunctions.class, "arrayMax", List.class),
    ARRAY_MIN(SqlFunctions.class, "arrayMin", List.class),
    ARRAY_POSITION(SqlFunctions.class, "arrayPosition", List.class, Object.class),
    ARRAY_PREPEND(SqlFunctions.class, "arrayPrepend", List.class, Object.class),
    ARRAY_REMOVE(SqlFunctions.class, "arrayRemove", List.class, Object.class),
    ARRAY_REPEAT(SqlFunctions.class, "arrayRepeat", Object.class, Integer.class),
    ARRAY_EXCEPT(SqlFunctions.class, "arrayExcept", List.class, List.class),
    ARRAY_INSERT(SqlFunctions.class, "arrayInsert", List.class, Integer.class, Object.class),
    ARRAY_INTERSECT(SqlFunctions.class, "arrayIntersect", List.class, List.class),
    ARRAY_UNION(SqlFunctions.class, "arrayUnion", List.class, List.class),
    ARRAY_REVERSE(SqlFunctions.class, "reverse", List.class),
    ARRAY_SLICE(SqlFunctions.class, "arraySlice", List.class, int.class, int.class),
    ARRAYS_OVERLAP(SqlFunctions.class, "arraysOverlap", List.class, List.class),
    ARRAYS_ZIP(SqlFunctions.class, "arraysZip", List.class, List.class),
    EXISTS(SqlFunctions.class, "exists", List.class, Function1.class),
    SORT_ARRAY(SqlFunctions.class, "sortArray", List.class, boolean.class),
    MAP(SqlFunctions.class, "map", Object[].class),
    MAP_CONCAT(SqlFunctions.class, "mapConcat", Map[].class),
    MAP_CONTAINS_KEY(SqlFunctions.class, "mapContainsKey", Map.class, Object.class),
    MAP_ENTRIES(SqlFunctions.class, "mapEntries", Map.class),
    MAP_KEYS(SqlFunctions.class, "mapKeys", Map.class),
    MAP_VALUES(SqlFunctions.class, "mapValues", Map.class),
    MAP_FROM_ARRAYS(SqlFunctions.class, "mapFromArrays", List.class, List.class),
    MAP_FROM_ENTRIES(SqlFunctions.class, "mapFromEntries", List.class),
    STR_TO_MAP(SqlFunctions.class, "strToMap", String.class, String.class, String.class),
    SUBSTRING_INDEX(SqlFunctions.class, "substringIndex", String.class, String.class, int.class),
    SELECTIVITY(Selectivity.class, "getSelectivity", RexNode.class),
    UNIQUE_KEYS(UniqueKeys.class, "getUniqueKeys", boolean.class),
    AVERAGE_ROW_SIZE(Size.class, "averageRowSize"),
    AVERAGE_COLUMN_SIZES(Size.class, "averageColumnSizes"),
    IS_PHASE_TRANSITION(Parallelism.class, "isPhaseTransition"),
    SPLIT_COUNT(Parallelism.class, "splitCount"),
    LOWER_BOUND_COST(LowerBoundCost.class, "getLowerBoundCost", VolcanoPlanner.class),
    MEMORY(Memory.class, "memory"),
    CUMULATIVE_MEMORY_WITHIN_PHASE(Memory.class, "cumulativeMemoryWithinPhase"),
    CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT(Memory.class, "cumulativeMemoryWithinPhaseSplit"),
    IS_MEASURE(Measure.class, "isMeasure", int.class),
    MEASURE_EXPAND(Measure.class, "expand", int.class, Measure.Context.class),
    COLUMN_UNIQUENESS(
            ColumnUniqueness.class, "areColumnsUnique", ImmutableBitSet.class, boolean.class),
    COLLATIONS(Collation.class, "collations"),
    DISTRIBUTION(Distribution.class, "distribution"),
    NODE_TYPES(NodeTypes.class, "getNodeTypes"),
    ROW_COUNT(RowCount.class, "getRowCount"),
    MAX_ROW_COUNT(MaxRowCount.class, "getMaxRowCount"),
    MIN_ROW_COUNT(MinRowCount.class, "getMinRowCount"),
    DISTINCT_ROW_COUNT(
            DistinctRowCount.class, "getDistinctRowCount", ImmutableBitSet.class, RexNode.class),
    PERCENTAGE_ORIGINAL_ROWS(PercentageOriginalRows.class, "getPercentageOriginalRows"),
    POPULATION_SIZE(PopulationSize.class, "getPopulationSize", ImmutableBitSet.class),
    COLUMN_ORIGIN(ColumnOrigin.class, "getColumnOrigins", int.class),
    EXPRESSION_LINEAGE(ExpressionLineage.class, "getExpressionLineage", RexNode.class),
    TABLE_REFERENCES(TableReferences.class, "getTableReferences"),
    CUMULATIVE_COST(CumulativeCost.class, "getCumulativeCost"),
    NON_CUMULATIVE_COST(NonCumulativeCost.class, "getNonCumulativeCost"),
    PREDICATES(Predicates.class, "getPredicates"),
    ALL_PREDICATES(AllPredicates.class, "getAllPredicates"),
    EXPLAIN_VISIBILITY(ExplainVisibility.class, "isVisibleInExplain", SqlExplainLevel.class),
    SCALAR_EXECUTE1(Scalar.class, "execute", Context.class),
    SCALAR_EXECUTE2(Scalar.class, "execute", Context.class, Object[].class),
    CONTEXT_VALUES(Context.class, "values", true),
    CONTEXT_ROOT(Context.class, "root", true),
    FUNCTION_CONTEXTS_OF(FunctionContexts.class, "of", DataContext.class, Object[].class),
    DATA_CONTEXT_GET_QUERY_PROVIDER(DataContext.class, "getQueryProvider"),
    METADATA_REL(Metadata.class, "rel"),
    STRUCT_ACCESS(SqlFunctions.class, "structAccess", Object.class, int.class, String.class),
    SOURCE_SORTER(SourceSorter.class, Function2.class, Function1.class, Comparator.class),
    BASIC_LAZY_ACCUMULATOR(BasicLazyAccumulator.class, Function2.class),
    LAZY_AGGREGATE_LAMBDA_FACTORY(LazyAggregateLambdaFactory.class, Function0.class, List.class),
    BASIC_AGGREGATE_LAMBDA_FACTORY(BasicAggregateLambdaFactory.class, Function0.class, List.class),
    AGG_LAMBDA_FACTORY_ACC_INITIALIZER(AggregateLambdaFactory.class, "accumulatorInitializer"),
    AGG_LAMBDA_FACTORY_ACC_ADDER(AggregateLambdaFactory.class, "accumulatorAdder"),
    AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR(
            AggregateLambdaFactory.class, "resultSelector", Function2.class),
    AGG_LAMBDA_FACTORY_ACC_SINGLE_GROUP_RESULT_SELECTOR(
            AggregateLambdaFactory.class, "singleGroupResultSelector", Function1.class),
    TUMBLING(EnumUtils.class, "tumbling", Enumerable.class, Function1.class),
    HOPPING(
            EnumUtils.class,
            "hopping",
            Enumerator.class,
            int.class,
            long.class,
            long.class,
            long.class),
    SESSIONIZATION(
            EnumUtils.class, "sessionize", Enumerator.class, int.class, int.class, long.class),
    BIG_DECIMAL_ADD(BigDecimal.class, "add", BigDecimal.class),
    BIG_DECIMAL_NEGATE(BigDecimal.class, "negate"),
    COMPARE_TO(Comparable.class, "compareTo", Object.class),
    VARIANT_CREATE(
            VariantSqlValue.class,
            "create",
            RoundingMode.class,
            Object.class,
            RuntimeTypeInformation.class),
    VARIANT_CAST(VariantValue.class, "cast", RuntimeTypeInformation.class),
    TYPEOF(VariantValue.class, "getTypeString", VariantValue.class),
    VARIANT_ITEM(SqlFunctions.class, "item", VariantValue.class, Object.class),
    VARIANTNULL(VariantNull.class, "getInstance"),
    FUNCTIONAL_DEPENDENCY(FunctionalDependency.class, "determines", int.class, int.class),
    FUNCTIONAL_DEPENDENCY_SET(
            FunctionalDependency.class,
            "determinesSet",
            ImmutableBitSet.class,
            ImmutableBitSet.class),
    FUNCTIONAL_DEPENDENCY_DEPENDENTS(
            FunctionalDependency.class, "dependents", ImmutableBitSet.class),
    FUNCTIONAL_DEPENDENCY_DETERMINANTS(
            FunctionalDependency.class, "determinants", ImmutableBitSet.class),
    FUNCTIONAL_DEPENDENCY_GET_FDS(FunctionalDependency.class, "getFDs");

    @SuppressWarnings("ImmutableEnumChecker")
    public final Method method;

    @SuppressWarnings("ImmutableEnumChecker")
    public final Constructor constructor;

    @SuppressWarnings("ImmutableEnumChecker")
    public final Field field;

    public static final ImmutableMap<Method, BuiltInMethod> FUNCTIONS_MAPS;

    static {
        final ImmutableMap.Builder<Method, BuiltInMethod> builder = ImmutableMap.builder();
        for (BuiltInMethod value : BuiltInMethod.values()) {
            if (value.method != null) {
                builder.put(value.method, value);
            }
        }
        FUNCTIONS_MAPS = builder.build();
    }

    BuiltInMethod(
            @Nullable Method method, @Nullable Constructor constructor, @Nullable Field field) {
        // TODO: split enum in three different ones
        this.method = castNonNull(method);
        this.constructor = castNonNull(constructor);
        this.field = castNonNull(field);
    }

    /** Defines a method. */
    BuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /** Defines a constructor. */
    BuiltInMethod(Class clazz, Class... argumentTypes) {
        this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /** Defines a field. */
    BuiltInMethod(Class clazz, String fieldName, boolean dummy) {
        this(null, null, Types.lookupField(clazz, fieldName));
        assert dummy : "dummy value for method overloading must be true";
    }

    public String getMethodName() {
        return castNonNull(method).getName();
    }
}
