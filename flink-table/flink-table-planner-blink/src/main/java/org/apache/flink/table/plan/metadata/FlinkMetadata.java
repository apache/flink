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

package org.apache.flink.table.plan.metadata;

import org.apache.flink.table.plan.stats.ValueInterval;
import org.apache.flink.table.plan.trait.FlinkRelDistribution;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.lang.reflect.Method;

/**
 * Contains the interfaces for several specified metadata of flink.
 */
public abstract class FlinkMetadata {

	/**
	 * Metadata about the interval of given column from a specified relational expression.
	 */
	public interface ColumnInterval extends Metadata {

		Method METHOD = Types.lookupMethod(ColumnInterval.class, "getColumnInterval", int.class);

		MetadataDef<ColumnInterval> DEF = MetadataDef.of(
				ColumnInterval.class,
				ColumnInterval.Handler.class,
				METHOD);

		/**
		 * Returns the interval value of the given column from a specified relational expression.
		 *
		 * @param index the index of the given column in a specified relational expression
		 * @return the interval of the given column.
		 *         Returns null if interval cannot be estimated,
		 *         Returns [[NullValueInterval]] if column values does not contains any value
		 *         except for null.
		 */
		ValueInterval getColumnInterval(int index);

		/**
		 * Handler API.
		 */
		interface Handler extends MetadataHandler<ColumnInterval> {
			ValueInterval getColumnInterval(RelNode r, RelMetadataQuery mq, int index);
		}
	}

	/**
	 * Metadata about how a relational expression is distributed.
	 *
	 * <p>If you are an operator consuming a relational expression, which subset
	 * of the rows are you seeing? You might be seeing all of them (BROADCAST
	 * or SINGLETON), only those whose key column values have a particular hash
	 * code (HASH) or only those whose column values have particular values or
	 * ranges of values (RANGE).
	 *
	 * <p>When a relational expression is partitioned, it is often partitioned
	 * among nodes, but it may be partitioned among threads running on the same
	 * node.
	 */
	public interface FlinkDistribution extends Metadata {
		Method METHOD = Types.lookupMethod(FlinkDistribution.class, "flinkDistribution");

		MetadataDef<FlinkDistribution> DEF = MetadataDef.of(
				FlinkDistribution.class,
				FlinkDistribution.Handler.class,
				METHOD);

		/** Determines how the rows are distributed. */
		FlinkRelDistribution flinkDistribution();

		/** Handler API. */
		interface Handler extends MetadataHandler<FlinkDistribution> {
			FlinkRelDistribution flinkDistribution(RelNode r, RelMetadataQuery mq);
		}
	}

}
