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

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * Intermediate step of a Join transformation. <br/>
 * To continue the Join transformation, select the join key of the first input {@link DataSet} by calling
 * {@link InnerJoinOperatorSets#where(int...)} or
 * {@link InnerJoinOperatorSets#where(KeySelector)}.
 *
 * @param <I1> The type of the first input DataSet of the Join transformation.
 * @param <I2> The type of the second input DataSet of the Join transformation.
 */
public final class InnerJoinOperatorSets<I1, I2> extends JoinOperatorSets<I1, I2> {

	public InnerJoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
		super(input1, input2);
	}

	public InnerJoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2, JoinHint hint) {
		super(input1, input2, hint);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return An incomplete Join transformation.
	 *           Call {@link InnerJoinOperatorSetsPredicate#equalTo(int...)} or
	 *           {@link InnerJoinOperatorSetsPredicate#equalTo(KeySelector)}
	 *           to continue the Join.
	 */
	@Override
	public InnerJoinOperatorSetsPredicate where(int... fields) {
		return new InnerJoinOperatorSetsPredicate(new Keys.ExpressionKeys<>(fields, input1.getType()));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return An incomplete Join transformation.
	 *           Call {@link InnerJoinOperatorSetsPredicate#equalTo(int...)} or
	 *           {@link InnerJoinOperatorSetsPredicate#equalTo(KeySelector)}
	 *           to continue the Join.
	 */
	@Override
	public InnerJoinOperatorSetsPredicate where(String... fields) {
		return new InnerJoinOperatorSetsPredicate(new Keys.ExpressionKeys<>(fields, input1.getType()));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return An incomplete Join transformation.
	 *           Call {@link InnerJoinOperatorSetsPredicate#equalTo(int...)} or
	 *           {@link InnerJoinOperatorSetsPredicate#equalTo(KeySelector)}
	 *           to continue the Join.
	 */
	@Override
	public <K> InnerJoinOperatorSetsPredicate where(KeySelector<I1, K> keySelector) {
		TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
		return new InnerJoinOperatorSetsPredicate(new Keys.SelectorFunctionKeys<>(keySelector, input1.getType(), keyType));
	}


	/**
	 * Intermediate step of a Join transformation. <br/>
	 * To continue the Join transformation, select the join key of the second input {@link DataSet} by calling
	 * {@link InnerJoinOperatorSetsPredicate#equalTo(int...)} or
	 * {@link InnerJoinOperatorSetsPredicate#equalTo(KeySelector)}.
	 */
	public class InnerJoinOperatorSetsPredicate extends JoinOperatorSetsPredicate {

		private InnerJoinOperatorSetsPredicate(Keys<I1> keys1) {
			super(keys1);
		}

		/**
		 * Continues a Join transformation and defines the {@link Tuple} fields of the second join
		 * {@link DataSet} that should be used as join keys.<br/>
		 * <b>Note: Fields can only be selected as join keys on Tuple DataSets.</b><br/>
		 * <p/>
		 * The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with
		 * the element of the first input being the first field of the tuple and the element of the
		 * second input being the second field of the tuple.
		 *
		 * @param fields The indexes of the Tuple fields of the second join DataSet that should be used as keys.
		 * @return A DefaultJoin that represents the joined DataSet.
		 */
		@Override
		public DefaultJoin<I1, I2> equalTo(int... fields) {
			return createDefaultJoin(new Keys.ExpressionKeys<>(fields, input2.getType()));
		}

		/**
		 * Continues a Join transformation and defines the fields of the second join
		 * {@link DataSet} that should be used as join keys.<br/>
		 * <p/>
		 * The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with
		 * the element of the first input being the first field of the tuple and the element of the
		 * second input being the second field of the tuple.
		 *
		 * @param fields The fields of the second join DataSet that should be used as keys.
		 * @return A DefaultJoin that represents the joined DataSet.
		 */
		@Override
		public DefaultJoin<I1, I2> equalTo(String... fields) {
			return createDefaultJoin(new Keys.ExpressionKeys<>(fields, input2.getType()));
		}

		/**
		 * Continues a Join transformation and defines a {@link KeySelector} function for the second join {@link DataSet}.</br>
		 * The KeySelector function is called for each element of the second DataSet and extracts a single
		 * key value on which the DataSet is joined. </br>
		 * <p/>
		 * The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with
		 * the element of the first input being the first field of the tuple and the element of the
		 * second input being the second field of the tuple.
		 *
		 * @param keySelector The KeySelector function which extracts the key values from the second DataSet on which it is joined.
		 * @return A DefaultJoin that represents the joined DataSet.
		 */
		@Override
		public <K> DefaultJoin<I1, I2> equalTo(KeySelector<I2, K> keySelector) {
			TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
			return createDefaultJoin(new Keys.SelectorFunctionKeys<>(keySelector, input2.getType(), keyType));
		}
	}
}
