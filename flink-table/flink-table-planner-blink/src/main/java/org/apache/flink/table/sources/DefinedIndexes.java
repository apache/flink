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

package org.apache.flink.table.sources;

import java.util.Collection;

/**
 * The {@link DefinedIndexes} interface can extends a {@link TableSource} to specify the
 * indexes meta information.
 *
 * <p>An Index can be a Unique Index or Normal Index. An Unique Index is similar to primary
 * key which defines a column or a group of columns that uniquely identifies each row in
 * a table or stream. An Normal Index is an index on the defined columns used to accelerate
 * querying.
 */
public interface DefinedIndexes {

	/**
	 * Returns the list of {@link TableIndex}s. Returns empty collection or null if no
	 * index is exist.
	 */
	Collection<TableIndex> getIndexes();

}
