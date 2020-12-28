/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.operator.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.operator.AlgoOperator;
import org.apache.flink.ml.operator.batch.source.TableSourceBatchOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

/**
 * Base class of batch algorithm operators.
 *
 * <p>This class extends {@link AlgoOperator} to support data transmission between BatchOperators.
 */
public abstract class BatchOperator<T extends BatchOperator<T>> extends AlgoOperator<T> {

	public BatchOperator() {
		super();
	}

	/**
	 * The constructor of BatchOperator with {@link Params}.
	 * @param params the initial Params.
	 */
	public BatchOperator(Params params) {
		super(params);
	}

	/**
	 * Link to another {@link BatchOperator}.
	 *
	 * <p>Link the <code>next</code> BatchOperator using this BatchOperator as its input.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * BatchOperator a = ...;
	 * BatchOperator b = ...;
	 * BatchOperator c = a.link(b)
	 * }
	 * </pre>
	 *
	 * <p>The BatchOperator <code>c</code> in the above code
	 * is the same instance as <code>b</code> which takes
	 * <code>a</code> as its input.
	 * Note that BatchOperator <code>b</code> will be changed
	 * to link from BatchOperator <code>a</code>.
	 *
	 * @param next The operator that will be modified to add this operator to its input.
	 * @param <B>  type of BatchOperator returned
	 * @return the linked next
	 * @see #linkFrom(BatchOperator[])
	 */
	public <B extends BatchOperator<?>> B link(B next) {
		next.linkFrom(this);
		return next;
	}

	/**
	 * Link from others {@link BatchOperator}.
	 *
	 * <p>Link this object to BatchOperator using the BatchOperators as its input.
	 *
	 * <p>For example:
	 *
	 * <pre>
	 * {@code
	 * BatchOperator a = ...;
	 * BatchOperator b = ...;
	 * BatchOperator c = ...;
	 *
	 * BatchOperator d = c.linkFrom(a, b)
	 * }
	 * </pre>
	 *
	 * <p>The <code>d</code> in the above code is the same
	 * instance as BatchOperator <code>c</code> which takes
	 * both <code>a</code> and <code>b</code> as its input.
	 *
	 * <p>note: It is not recommended to linkFrom itself or linkFrom the same group inputs twice.
	 *
	 * @param inputs the linked inputs
	 * @return the linked this object
	 */
	public abstract T linkFrom(BatchOperator<?>... inputs);

	/**
	 * Get the {@link DataSet} that casted from the output table with the type of {@link Row}.
	 *
	 * @return the casted {@link DataSet}
	 */
	public DataSet<Row> getDataSet() {
		return RowTypeDataSet.fromTable(getMLEnvironmentId(), getOutput());
	}

	@Override
	public BatchOperator<?> select(String fields) {
		return BatchSqlOperators.select(this, fields);
	}

	@Override
	public BatchOperator<?> select(String[] fields) {
		return select(TableUtil.columnsToSqlClause(fields));
	}

	@Override
	public BatchOperator<?> as(String fields) {
		return BatchSqlOperators.as(this, fields);
	}

	@Override
	public BatchOperator<?> where(String predicate) {
		return BatchSqlOperators.where(this, predicate);
	}

	@Override
	public BatchOperator<?> filter(String predicate) {
		return BatchSqlOperators.filter(this, predicate);
	}

	/**
	 * Remove duplicated records.
	 *
	 * @return The resulted <code>BatchOperator</code> of the "distinct" operation.
	 */
	public BatchOperator distinct() {
		return BatchSqlOperators.distinct(this);
	}

	/**
	 * Order the records by a specific field.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public BatchOperator orderBy(String fieldName) {
		return BatchSqlOperators.orderBy(this, fieldName);
	}

	/**
	 * Order the records by a specific field and keeping a limited number of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param limit     The maximum number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public BatchOperator orderBy(String fieldName, int limit) {
		return BatchSqlOperators.orderBy(this, fieldName, limit);
	}

	/**
	 * Order the records by a specific field and keeping a specific range of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param offset    The starting position of records to keep.
	 * @param fetch     The  number of records to keep.
	 * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
	 */
	public BatchOperator orderBy(String fieldName, int offset, int fetch) {
		return BatchSqlOperators.orderBy(this, fieldName, offset, fetch);
	}

	/**
	 * Apply the "group by" operation.
	 *
	 * @param groupByPredicate The predicate specifying the fields from which records are grouped.
	 * @param selectClause     The clause specifying the fields to select and the aggregation operations.
	 * @return The resulted <code>BatchOperator</code> of the "groupBy" operation.
	 */
	public BatchOperator groupBy(String groupByPredicate, String selectClause) {
		return BatchSqlOperators.groupBy(this, groupByPredicate, selectClause);
	}

	/**
	 * Join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @return The resulted <code>BatchOperator</code> of the "join" operation.
	 */
	public BatchOperator join(BatchOperator rightOp, String joinPredicate) {
		return join(rightOp, joinPredicate, "*");
	}

	/**
	 * Join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @param selectClause  The clause specifying the fields to select.
	 * @return The resulted <code>BatchOperator</code> of the "join" operation.
	 */
	public BatchOperator join(BatchOperator rightOp, String joinPredicate, String selectClause) {
		return BatchSqlOperators.join(this, rightOp, joinPredicate, selectClause);
	}

	/**
	 * Left outer join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @return The resulted <code>BatchOperator</code> of the "left outer join" operation.
	 */
	public BatchOperator leftOuterJoin(BatchOperator rightOp, String joinPredicate) {
		return leftOuterJoin(rightOp, joinPredicate, "*");
	}

	/**
	 * Left outer join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @param selectClause  The clause specifying the fields to select.
	 * @return The resulted <code>BatchOperator</code> of the "left outer join" operation.
	 */
	public BatchOperator leftOuterJoin(BatchOperator rightOp, String joinPredicate, String selectClause) {
		return BatchSqlOperators.leftOuterJoin(this, rightOp, joinPredicate, selectClause);
	}

	/**
	 * Right outer join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @return The resulted <code>BatchOperator</code> of the "right outer join" operation.
	 */
	public BatchOperator rightOuterJoin(BatchOperator rightOp, String joinPredicate) {
		return rightOuterJoin(rightOp, joinPredicate, "*");
	}

	/**
	 * Right outer join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @param selectClause  The clause specifying the fields to select.
	 * @return The resulted <code>BatchOperator</code> of the "right outer join" operation.
	 */
	public BatchOperator rightOuterJoin(BatchOperator rightOp, String joinPredicate, String selectClause) {
		return BatchSqlOperators.rightOuterJoin(this, rightOp, joinPredicate, selectClause);
	}

	/**
	 * Full outer join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @return The resulted <code>BatchOperator</code> of the "full outer join" operation.
	 */
	public BatchOperator fullOuterJoin(BatchOperator rightOp, String joinPredicate) {
		return fullOuterJoin(rightOp, joinPredicate, "*");
	}

	/**
	 * Full outer join with another <code>BatchOperator</code>.
	 *
	 * @param rightOp       Another <code>BatchOperator</code> to join with.
	 * @param joinPredicate The predicate specifying the join conditions.
	 * @param selectClause  The clause specifying the fields to select.
	 * @return The resulted <code>BatchOperator</code> of the "full outer join" operation.
	 */
	public BatchOperator fullOuterJoin(BatchOperator rightOp, String joinPredicate, String selectClause) {
		return BatchSqlOperators.fullOuterJoin(this, rightOp, joinPredicate, selectClause);
	}

	/**
	 * Union with another <code>BatchOperator</code>, the duplicated records are removed.
	 *
	 * @param rightOp Another <code>BatchOperator</code> to union with.
	 * @return The resulted <code>BatchOperator</code> of the "union" operation.
	 */
	public BatchOperator union(BatchOperator rightOp) {
		return BatchSqlOperators.union(this, rightOp);
	}

	/**
	 * Union with another <code>BatchOperator</code>, the duplicated records are kept.
	 *
	 * @param rightOp Another <code>BatchOperator</code> to union with.
	 * @return The resulted <code>BatchOperator</code> of the "unionAll" operation.
	 */
	public BatchOperator unionAll(BatchOperator rightOp) {
		return BatchSqlOperators.unionAll(this, rightOp);
	}

	/**
	 * Intersect with another <code>BatchOperator</code>, the duplicated records are removed.
	 *
	 * @param rightOp Another <code>BatchOperator</code> to intersect with.
	 * @return The resulted <code>BatchOperator</code> of the "intersect" operation.
	 */
	public BatchOperator intersect(BatchOperator rightOp) {
		return BatchSqlOperators.intersect(this, rightOp);
	}

	/**
	 * Intersect with another <code>BatchOperator</code>, the duplicated records are kept.
	 *
	 * @param rightOp Another <code>BatchOperator</code> to intersect with.
	 * @return The resulted <code>BatchOperator</code> of the "intersect" operation.
	 */
	public BatchOperator intersectAll(BatchOperator rightOp) {
		return BatchSqlOperators.intersectAll(this, rightOp);
	}

	/**
	 * Minus with another <code>BatchOperator</code>, the duplicated records are removed.
	 *
	 * @param rightOp Another <code>BatchOperator</code> to minus with.
	 * @return The resulted <code>BatchOperator</code> of the "minus" operation.
	 */
	public BatchOperator minus(BatchOperator rightOp) {
		return BatchSqlOperators.minus(this, rightOp);
	}

	/**
	 * Minus with another <code>BatchOperator</code>, the duplicated records are kept.
	 *
	 * @param rightOp Another <code>BatchOperator</code> to minus with.
	 * @return The resulted <code>BatchOperator</code> of the "minus" operation.
	 */
	public BatchOperator minusAll(BatchOperator rightOp) {
		return BatchSqlOperators.minusAll(this, rightOp);
	}


	/**
	 * Cross with another BatchOperator.
	 *
	 * @param rightOp The BatchOperator to cross with.
	 * @return The cross result.
	 */
	public BatchOperator cross(BatchOperator rightOp) {
		return BatchSqlOperators.cross(this, rightOp);
	}

	/**
	 * Cross with another BatchOperator whose size is tiny.
	 *
	 * @param rightOp The BatchOperator to cross with.
	 * @return The cross result.
	 */
	public BatchOperator crossWithTiny(BatchOperator rightOp) {
		return BatchSqlOperators.crossWithTiny(this, rightOp);
	}

	/**
	 * Cross with another BatchOperator whose size is huge.
	 *
	 * @param rightOp The BatchOperator to cross with.
	 * @return The cross result.
	 */
	public BatchOperator crossWithHuge(BatchOperator rightOp) {
		return BatchSqlOperators.crossWithHuge(this, rightOp);
	}

	/**
	 * create a new BatchOperator from table.
	 * @param table the input table
	 * @return the new BatchOperator
	 */
	public static BatchOperator<?> fromTable(Table table) {
		return new TableSourceBatchOp(table);
	}

	protected static BatchOperator<?> checkAndGetFirst(BatchOperator<?> ... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}
}
