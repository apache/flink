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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * This operator represents the application of a projection operation on a data set, and the result
 * data set produced by the function.
 *
 * @param <IN> The type of the data set projected by the operator.
 * @param <OUT> The type of data set that is the result of the projection.
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public class ProjectOperator<IN, OUT extends Tuple>
        extends SingleInputOperator<IN, OUT, ProjectOperator<IN, OUT>> {

    protected final int[] fields;

    public ProjectOperator(DataSet<IN> input, int[] fields, TupleTypeInfo<OUT> returnType) {
        super(input, returnType);

        this.fields = fields;
    }

    @Override
    protected org.apache.flink.api.common.operators.base.MapOperatorBase<
                    IN, OUT, MapFunction<IN, OUT>>
            translateToDataFlow(Operator<IN> input) {
        String name = getName() != null ? getName() : "Projection " + Arrays.toString(fields);
        // create operator
        PlanProjectOperator<IN, OUT> ppo =
                new PlanProjectOperator<IN, OUT>(
                        fields, name, getInputType(), getResultType(), context.getConfig());
        // set input
        ppo.setInput(input);
        // set parallelism
        ppo.setParallelism(this.getParallelism());
        ppo.setSemanticProperties(
                SemanticPropUtil.createProjectionPropertiesSingle(
                        fields, (CompositeType<?>) getInputType()));

        return ppo;
    }

    /** @deprecated Deprecated method only kept for compatibility. */
    @SuppressWarnings("unchecked")
    @Deprecated
    @PublicEvolving
    public <R extends Tuple> ProjectOperator<IN, R> types(Class<?>... types) {
        TupleTypeInfo<R> typeInfo = (TupleTypeInfo<R>) this.getResultType();

        if (types.length != typeInfo.getArity()) {
            throw new InvalidProgramException("Provided types do not match projection.");
        }
        for (int i = 0; i < types.length; i++) {
            Class<?> typeClass = types[i];
            if (!typeClass.equals(typeInfo.getTypeAt(i).getTypeClass())) {
                throw new InvalidProgramException(
                        "Provided type "
                                + typeClass.getSimpleName()
                                + " at position "
                                + i
                                + " does not match projection");
            }
        }
        return (ProjectOperator<IN, R>) this;
    }

    /**
     * A projection of {@link DataSet}.
     *
     * @param <T>
     */
    @Internal
    public static class Projection<T> {

        private final DataSet<T> ds;
        private int[] fieldIndexes;

        public Projection(DataSet<T> ds, int[] fieldIndexes) {

            if (!(ds.getType() instanceof TupleTypeInfo)) {
                throw new UnsupportedOperationException(
                        "project() can only be applied to DataSets of Tuples.");
            }

            if (fieldIndexes.length == 0) {
                throw new IllegalArgumentException(
                        "project() needs to select at least one (1) field.");
            } else if (fieldIndexes.length > Tuple.MAX_ARITY - 1) {
                throw new IllegalArgumentException(
                        "project() may select only up to (" + (Tuple.MAX_ARITY - 1) + ") fields.");
            }

            int maxFieldIndex = ds.getType().getArity();
            for (int fieldIndexe : fieldIndexes) {
                Preconditions.checkElementIndex(fieldIndexe, maxFieldIndex);
            }

            this.ds = ds;
            this.fieldIndexes = fieldIndexes;
        }

        // --------------------------------------------------------------------------------------------
        // The following lines are generated.
        // --------------------------------------------------------------------------------------------
        // BEGIN_OF_TUPLE_DEPENDENT_CODE
        // GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.

        /**
         * Chooses a projectTupleX according to the length of {@link
         * org.apache.flink.api.java.operators.ProjectOperator.Projection#fieldIndexes}.
         *
         * @return The projected DataSet.
         * @see org.apache.flink.api.java.operators.ProjectOperator.Projection
         */
        @SuppressWarnings("unchecked")
        public <OUT extends Tuple> ProjectOperator<T, OUT> projectTupleX() {
            ProjectOperator<T, OUT> projOperator;

            switch (fieldIndexes.length) {
                case 1:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple1();
                    break;
                case 2:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple2();
                    break;
                case 3:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple3();
                    break;
                case 4:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple4();
                    break;
                case 5:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple5();
                    break;
                case 6:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple6();
                    break;
                case 7:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple7();
                    break;
                case 8:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple8();
                    break;
                case 9:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple9();
                    break;
                case 10:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple10();
                    break;
                case 11:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple11();
                    break;
                case 12:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple12();
                    break;
                case 13:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple13();
                    break;
                case 14:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple14();
                    break;
                case 15:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple15();
                    break;
                case 16:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple16();
                    break;
                case 17:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple17();
                    break;
                case 18:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple18();
                    break;
                case 19:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple19();
                    break;
                case 20:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple20();
                    break;
                case 21:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple21();
                    break;
                case 22:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple22();
                    break;
                case 23:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple23();
                    break;
                case 24:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple24();
                    break;
                case 25:
                    projOperator = (ProjectOperator<T, OUT>) projectTuple25();
                    break;
                default:
                    throw new IllegalStateException("Excessive arity in tuple.");
            }

            return projOperator;
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0> ProjectOperator<T, Tuple1<T0>> projectTuple1() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

            return new ProjectOperator<T, Tuple1<T0>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1> ProjectOperator<T, Tuple2<T0, T1>> projectTuple2() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

            return new ProjectOperator<T, Tuple2<T0, T1>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2> ProjectOperator<T, Tuple3<T0, T1, T2>> projectTuple3() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

            return new ProjectOperator<T, Tuple3<T0, T1, T2>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3> ProjectOperator<T, Tuple4<T0, T1, T2, T3>> projectTuple4() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType =
                    new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

            return new ProjectOperator<T, Tuple4<T0, T1, T2, T3>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4> ProjectOperator<T, Tuple5<T0, T1, T2, T3, T4>> projectTuple5() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType =
                    new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

            return new ProjectOperator<T, Tuple5<T0, T1, T2, T3, T4>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5>
                ProjectOperator<T, Tuple6<T0, T1, T2, T3, T4, T5>> projectTuple6() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType =
                    new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

            return new ProjectOperator<T, Tuple6<T0, T1, T2, T3, T4, T5>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6>
                ProjectOperator<T, Tuple7<T0, T1, T2, T3, T4, T5, T6>> projectTuple7() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType =
                    new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

            return new ProjectOperator<T, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7>
                ProjectOperator<T, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> projectTuple8() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType =
                    new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

            return new ProjectOperator<T, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8>
                ProjectOperator<T, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> projectTuple9() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType =
                    new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

            return new ProjectOperator<T, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>
                ProjectOperator<T, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>
                        projectTuple10() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType =
                    new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

            return new ProjectOperator<T, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
                ProjectOperator<T, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>
                        projectTuple11() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType =
                    new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

            return new ProjectOperator<T, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>
                ProjectOperator<T, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>
                        projectTuple12() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType =
                    new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(
                            fTypes);

            return new ProjectOperator<
                    T, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>
                ProjectOperator<T, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>
                        projectTuple13() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType =
                    new TupleTypeInfo<
                            Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

            return new ProjectOperator<
                    T, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>
                ProjectOperator<
                                T,
                                Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>
                        projectTuple14() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple14<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13>>(fTypes);

            return new ProjectOperator<
                    T, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>
                ProjectOperator<
                                T,
                                Tuple15<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14>>
                        projectTuple15() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple15<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14>>(fTypes);

            return new ProjectOperator<
                    T, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>
                ProjectOperator<
                                T,
                                Tuple16<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15>>
                        projectTuple16() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple16<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple16<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(
                    this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>
                ProjectOperator<
                                T,
                                Tuple17<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16>>
                        projectTuple17() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple17<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple17<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple17<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>
                ProjectOperator<
                                T,
                                Tuple18<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17>>
                        projectTuple18() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple18<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple18<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple18<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>
                ProjectOperator<
                                T,
                                Tuple19<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18>>
                        projectTuple19() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple19<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple19<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple19<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <
                        T0,
                        T1,
                        T2,
                        T3,
                        T4,
                        T5,
                        T6,
                        T7,
                        T8,
                        T9,
                        T10,
                        T11,
                        T12,
                        T13,
                        T14,
                        T15,
                        T16,
                        T17,
                        T18,
                        T19>
                ProjectOperator<
                                T,
                                Tuple20<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18,
                                        T19>>
                        projectTuple20() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple20<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18,
                                    T19>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple20<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18,
                                            T19>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple20<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18,
                            T19>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <
                        T0,
                        T1,
                        T2,
                        T3,
                        T4,
                        T5,
                        T6,
                        T7,
                        T8,
                        T9,
                        T10,
                        T11,
                        T12,
                        T13,
                        T14,
                        T15,
                        T16,
                        T17,
                        T18,
                        T19,
                        T20>
                ProjectOperator<
                                T,
                                Tuple21<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18,
                                        T19,
                                        T20>>
                        projectTuple21() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple21<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18,
                                    T19,
                                    T20>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple21<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18,
                                            T19,
                                            T20>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple21<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18,
                            T19,
                            T20>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <
                        T0,
                        T1,
                        T2,
                        T3,
                        T4,
                        T5,
                        T6,
                        T7,
                        T8,
                        T9,
                        T10,
                        T11,
                        T12,
                        T13,
                        T14,
                        T15,
                        T16,
                        T17,
                        T18,
                        T19,
                        T20,
                        T21>
                ProjectOperator<
                                T,
                                Tuple22<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18,
                                        T19,
                                        T20,
                                        T21>>
                        projectTuple22() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple22<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18,
                                    T19,
                                    T20,
                                    T21>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple22<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18,
                                            T19,
                                            T20,
                                            T21>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple22<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18,
                            T19,
                            T20,
                            T21>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <
                        T0,
                        T1,
                        T2,
                        T3,
                        T4,
                        T5,
                        T6,
                        T7,
                        T8,
                        T9,
                        T10,
                        T11,
                        T12,
                        T13,
                        T14,
                        T15,
                        T16,
                        T17,
                        T18,
                        T19,
                        T20,
                        T21,
                        T22>
                ProjectOperator<
                                T,
                                Tuple23<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18,
                                        T19,
                                        T20,
                                        T21,
                                        T22>>
                        projectTuple23() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple23<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18,
                                    T19,
                                    T20,
                                    T21,
                                    T22>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple23<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18,
                                            T19,
                                            T20,
                                            T21,
                                            T22>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple23<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18,
                            T19,
                            T20,
                            T21,
                            T22>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <
                        T0,
                        T1,
                        T2,
                        T3,
                        T4,
                        T5,
                        T6,
                        T7,
                        T8,
                        T9,
                        T10,
                        T11,
                        T12,
                        T13,
                        T14,
                        T15,
                        T16,
                        T17,
                        T18,
                        T19,
                        T20,
                        T21,
                        T22,
                        T23>
                ProjectOperator<
                                T,
                                Tuple24<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18,
                                        T19,
                                        T20,
                                        T21,
                                        T22,
                                        T23>>
                        projectTuple24() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple24<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18,
                                    T19,
                                    T20,
                                    T21,
                                    T22,
                                    T23>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple24<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18,
                                            T19,
                                            T20,
                                            T21,
                                            T22,
                                            T23>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple24<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18,
                            T19,
                            T20,
                            T21,
                            T22,
                            T23>>(this.ds, this.fieldIndexes, tType);
        }

        /**
         * Projects a {@link Tuple} {@link DataSet} to the previously selected fields.
         *
         * @return The projected DataSet.
         * @see Tuple
         * @see DataSet
         */
        public <
                        T0,
                        T1,
                        T2,
                        T3,
                        T4,
                        T5,
                        T6,
                        T7,
                        T8,
                        T9,
                        T10,
                        T11,
                        T12,
                        T13,
                        T14,
                        T15,
                        T16,
                        T17,
                        T18,
                        T19,
                        T20,
                        T21,
                        T22,
                        T23,
                        T24>
                ProjectOperator<
                                T,
                                Tuple25<
                                        T0,
                                        T1,
                                        T2,
                                        T3,
                                        T4,
                                        T5,
                                        T6,
                                        T7,
                                        T8,
                                        T9,
                                        T10,
                                        T11,
                                        T12,
                                        T13,
                                        T14,
                                        T15,
                                        T16,
                                        T17,
                                        T18,
                                        T19,
                                        T20,
                                        T21,
                                        T22,
                                        T23,
                                        T24>>
                        projectTuple25() {
            TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
            TupleTypeInfo<
                            Tuple25<
                                    T0,
                                    T1,
                                    T2,
                                    T3,
                                    T4,
                                    T5,
                                    T6,
                                    T7,
                                    T8,
                                    T9,
                                    T10,
                                    T11,
                                    T12,
                                    T13,
                                    T14,
                                    T15,
                                    T16,
                                    T17,
                                    T18,
                                    T19,
                                    T20,
                                    T21,
                                    T22,
                                    T23,
                                    T24>>
                    tType =
                            new TupleTypeInfo<
                                    Tuple25<
                                            T0,
                                            T1,
                                            T2,
                                            T3,
                                            T4,
                                            T5,
                                            T6,
                                            T7,
                                            T8,
                                            T9,
                                            T10,
                                            T11,
                                            T12,
                                            T13,
                                            T14,
                                            T15,
                                            T16,
                                            T17,
                                            T18,
                                            T19,
                                            T20,
                                            T21,
                                            T22,
                                            T23,
                                            T24>>(fTypes);

            return new ProjectOperator<
                    T,
                    Tuple25<
                            T0,
                            T1,
                            T2,
                            T3,
                            T4,
                            T5,
                            T6,
                            T7,
                            T8,
                            T9,
                            T10,
                            T11,
                            T12,
                            T13,
                            T14,
                            T15,
                            T16,
                            T17,
                            T18,
                            T19,
                            T20,
                            T21,
                            T22,
                            T23,
                            T24>>(this.ds, this.fieldIndexes, tType);
        }

        // END_OF_TUPLE_DEPENDENT_CODE
        // -----------------------------------------------------------------------------------------

        private TypeInformation<?>[] extractFieldTypes(int[] fields, TypeInformation<?> inType) {

            TupleTypeInfo<?> inTupleType = (TupleTypeInfo<?>) inType;
            TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];

            for (int i = 0; i < fields.length; i++) {
                fieldTypes[i] = inTupleType.getTypeAt(fields[i]);
            }

            return fieldTypes;
        }
    }
}
