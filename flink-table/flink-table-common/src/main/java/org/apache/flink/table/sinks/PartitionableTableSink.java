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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Experimental;

import java.util.Map;

/**
 * An interface for partitionable {@link TableSink}. A partitionable sink can writes
 * query results to partitions.
 *
 * <p>Partition columns are defined via catalog table.
 *
 * <p>For example, a partitioned table named {@code my_table} with a table schema
 * {@code [a INT, b VARCHAR, c DOUBLE, dt VARCHAR, country VARCHAR]} is partitioned on columns
 * {@code dt, country}. Then {@code dt} is the first partition column, and
 * {@code country} is the secondary partition column.
 *
 * <p>We can insert data into table partitions using INSERT INTO PARTITION syntax, for example:
 * <pre>
 * <code>
 *     INSERT INTO my_table PARTITION (dt='2019-06-20', country='bar') select a, b, c from my_view
 * </code>
 * </pre>
 * When all the partition columns are set a value in PARTITION clause, it is inserting into a
 * static partition. It will writes the query result into a static partition,
 * i.e. {@code dt='2019-06-20', country='bar'}. The user specified static partitions will be told
 * to the sink via {@link #setStaticPartition(Map)}.
 *
 * <p>The INSERT INTO PARTITION syntax also supports dynamic partition inserts.
 * <pre>
 * <code>
 *     INSERT INTO my_table PARTITION (dt='2019-06-20') select a, b, c, country from another_view
 * </code>
 * </pre>
 * When partial partition columns (prefix part of all partition columns) are set a value in
 * PARTITION clause, it is writing the query result into a dynamic partition. In the above example,
 * the static partition part is {@code dt='2019-06-20'} which will be told to the sink via
 * {@link #setStaticPartition(Map)}. And the {@code country} is the dynamic partition which will be
 * get from each record.
 */
@Experimental
public interface PartitionableTableSink {

	/**
	 * Sets the static partition into the {@link TableSink}. The static partition may be partial
	 * of all partition columns. See the class Javadoc for more details.
	 *
	 * <p>The static partition is represented as a {@code Map<String, String>} which maps from
	 * partition field name to partition value. The partition values are all encoded as strings,
	 * i.e. encoded using String.valueOf(...). For example, if we have a static partition
	 * {@code f0=1024, f1="foo", f2="bar"}. f0 is an integer type, f1 and f2 are string types.
	 * They will all be encoded as strings: "1024", "foo", "bar". And can be decoded to original
	 * literals based on the field types.
	 *
	 * @param partitions user specified static partition
	 */
	void setStaticPartition(Map<String, String> partitions);

	/**
	 * If returns true, sink can trust all records will definitely be grouped by partition fields
	 * before consumed by the {@link TableSink}, i.e. the sink will receive all elements of one
	 * partition and then all elements of another partition, elements of different partitions
	 * will not be mixed. For some sinks, this can be used to reduce number of the partition
	 * writers to improve writing performance.
	 *
	 * <p>This method is used to configure the behavior of input whether to be grouped by partition,
	 * if true, at the same time the sink should also configure itself, i.e. set an internal field
	 * that changes the writing behavior (writing one partition at a time).
	 *
	 * @param supportsGrouping whether the execution mode supports grouping,
	 *                            e.g. grouping (usually use sort to implement) is only supported
	 *                            in batch mode, not supported in streaming mode.
	 *
	 * @return whether data need to be grouped by partition before consumed by the sink. Default is false.
	 * If {@code supportsGrouping} is false, it should never return true (requires grouping), otherwise it will fail.
	 */
	default boolean configurePartitionGrouping(boolean supportsGrouping) {
		return false;
	}
}
