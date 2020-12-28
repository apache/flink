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

package org.apache.flink.api.java.operators.join;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * Intermediate step of an Outer Join transformation.
 *
 * <p>To continue the Join transformation, select the join key of the first input {@link DataSet} by
 * calling {@link JoinOperatorSetsBase#where(int...)} or {@link
 * JoinOperatorSetsBase#where(KeySelector)}.
 *
 * @param <I1> The type of the first input DataSet of the Join transformation.
 * @param <I2> The type of the second input DataSet of the Join transformation.
 */
@Public
public class JoinOperatorSetsBase<I1, I2> {

    protected final DataSet<I1> input1;
    protected final DataSet<I2> input2;

    protected final JoinHint joinHint;
    protected final JoinType joinType;

    public JoinOperatorSetsBase(DataSet<I1> input1, DataSet<I2> input2) {
        this(input1, input2, JoinHint.OPTIMIZER_CHOOSES);
    }

    public JoinOperatorSetsBase(DataSet<I1> input1, DataSet<I2> input2, JoinHint hint) {
        this(input1, input2, hint, JoinType.INNER);
    }

    public JoinOperatorSetsBase(
            DataSet<I1> input1, DataSet<I2> input2, JoinHint hint, JoinType type) {
        if (input1 == null || input2 == null) {
            throw new NullPointerException();
        }

        this.input1 = input1;
        this.input2 = input2;
        this.joinHint = hint;
        this.joinType = type;
    }

    /**
     * Continues a Join transformation.
     *
     * <p>Defines the {@link Tuple} fields of the first join {@link DataSet} that should be used as
     * join keys.
     *
     * <p><b>Note: Fields can only be selected as join keys on Tuple DataSets.</b>
     *
     * @param fields The indexes of the other Tuple fields of the first join DataSets that should be
     *     used as keys.
     * @return An incomplete Join transformation. Call {@link
     *     org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(int...)}
     *     or {@link
     *     org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(KeySelector)}
     *     to continue the Join.
     * @see Tuple
     * @see DataSet
     */
    public JoinOperatorSetsPredicateBase where(int... fields) {
        return new JoinOperatorSetsPredicateBase(
                new Keys.ExpressionKeys<>(fields, input1.getType()));
    }

    /**
     * Continues a Join transformation.
     *
     * <p>Defines the fields of the first join {@link DataSet} that should be used as grouping keys.
     * Fields are the names of member fields of the underlying type of the data set.
     *
     * @param fields The fields of the first join DataSets that should be used as keys.
     * @return An incomplete Join transformation. Call {@link
     *     org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(int...)}
     *     or {@link
     *     org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(KeySelector)}
     *     to continue the Join.
     * @see Tuple
     * @see DataSet
     */
    public JoinOperatorSetsPredicateBase where(String... fields) {
        return new JoinOperatorSetsPredicateBase(
                new Keys.ExpressionKeys<>(fields, input1.getType()));
    }

    /**
     * Continues a Join transformation and defines a {@link KeySelector} function for the first join
     * {@link DataSet}.
     *
     * <p>The KeySelector function is called for each element of the first DataSet and extracts a
     * single key value on which the DataSet is joined.
     *
     * @param keySelector The KeySelector function which extracts the key values from the DataSet on
     *     which it is joined.
     * @return An incomplete Join transformation. Call {@link
     *     org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(int...)}
     *     or {@link
     *     org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(KeySelector)}
     *     to continue the Join.
     * @see KeySelector
     * @see DataSet
     */
    public <K> JoinOperatorSetsPredicateBase where(KeySelector<I1, K> keySelector) {
        TypeInformation<K> keyType =
                TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
        return new JoinOperatorSetsPredicateBase(
                new Keys.SelectorFunctionKeys<>(keySelector, input1.getType(), keyType));
    }

    /**
     * Intermediate step of a Join transformation.
     *
     * <p>To continue the Join transformation, select the join key of the second input {@link
     * DataSet} by calling {@link
     * org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(int...)}
     * or {@link
     * org.apache.flink.api.java.operators.join.JoinOperatorSetsBase.JoinOperatorSetsPredicateBase#equalTo(KeySelector)}.
     */
    public class JoinOperatorSetsPredicateBase {

        protected final Keys<I1> keys1;

        protected JoinOperatorSetsPredicateBase(Keys<I1> keys1) {
            if (keys1 == null) {
                throw new NullPointerException();
            }

            if (keys1.isEmpty()) {
                throw new InvalidProgramException("The join keys must not be empty.");
            }

            this.keys1 = keys1;
        }

        /**
         * Continues a Join transformation and defines the {@link Tuple} fields of the second join
         * {@link DataSet} that should be used as join keys.
         *
         * <p><b>Note: Fields can only be selected as join keys on Tuple DataSets.</b>
         *
         * <p>The resulting {@link JoinFunctionAssigner} needs to be finished by providing a {@link
         * JoinFunction} by calling {@link JoinFunctionAssigner#with(JoinFunction)}
         *
         * @param fields The indexes of the Tuple fields of the second join DataSet that should be
         *     used as keys.
         * @return A JoinFunctionAssigner.
         */
        public JoinFunctionAssigner<I1, I2> equalTo(int... fields) {
            return createJoinFunctionAssigner(new Keys.ExpressionKeys<>(fields, input2.getType()));
        }

        /**
         * Continues a Join transformation and defines the fields of the second join {@link DataSet}
         * that should be used as join keys.
         *
         * <p>The resulting {@link JoinFunctionAssigner} needs to be finished by providing a {@link
         * JoinFunction} by calling {@link JoinFunctionAssigner#with(JoinFunction)}
         *
         * @param fields The fields of the second join DataSet that should be used as keys.
         * @return A JoinFunctionAssigner.
         */
        public JoinFunctionAssigner<I1, I2> equalTo(String... fields) {
            return createJoinFunctionAssigner(new Keys.ExpressionKeys<>(fields, input2.getType()));
        }

        /**
         * Continues a Join transformation and defines a {@link KeySelector} function for the second
         * join {@link DataSet}.
         *
         * <p>The KeySelector function is called for each element of the second DataSet and extracts
         * a single key value on which the DataSet is joined.
         *
         * <p>The resulting {@link JoinFunctionAssigner} needs to be finished by providing a {@link
         * JoinFunction} by calling {@link JoinFunctionAssigner#with(JoinFunction)}
         *
         * @param keySelector The KeySelector function which extracts the key values from the second
         *     DataSet on which it is joined.
         * @return A JoinFunctionAssigner.
         */
        public <K> JoinFunctionAssigner<I1, I2> equalTo(KeySelector<I2, K> keySelector) {
            TypeInformation<K> keyType =
                    TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
            return createJoinFunctionAssigner(
                    new Keys.SelectorFunctionKeys<>(keySelector, input2.getType(), keyType));
        }

        protected JoinFunctionAssigner<I1, I2> createJoinFunctionAssigner(Keys<I2> keys2) {
            DefaultJoin<I1, I2> join = createDefaultJoin(keys2);
            return new DefaultJoinFunctionAssigner(join);
        }

        protected DefaultJoin<I1, I2> createDefaultJoin(Keys<I2> keys2) {
            if (keys2 == null) {
                throw new NullPointerException("The join keys may not be null.");
            }

            if (keys2.isEmpty()) {
                throw new InvalidProgramException("The join keys may not be empty.");
            }

            try {
                keys1.areCompatible(keys2);
            } catch (Keys.IncompatibleKeysException e) {
                throw new InvalidProgramException(
                        "The pair of join keys are not compatible with each other.", e);
            }
            return new DefaultJoin<>(
                    input1, input2, keys1, keys2, joinHint, Utils.getCallLocationName(4), joinType);
        }

        private class DefaultJoinFunctionAssigner implements JoinFunctionAssigner<I1, I2> {

            private final DefaultJoin<I1, I2> defaultJoin;

            public DefaultJoinFunctionAssigner(DefaultJoin<I1, I2> defaultJoin) {
                this.defaultJoin = defaultJoin;
            }

            public <R> EquiJoin<I1, I2, R> with(JoinFunction<I1, I2, R> joinFunction) {
                return defaultJoin.with(joinFunction);
            }

            public <R> EquiJoin<I1, I2, R> with(FlatJoinFunction<I1, I2, R> joinFunction) {
                return defaultJoin.with(joinFunction);
            }
        }
    }
}
