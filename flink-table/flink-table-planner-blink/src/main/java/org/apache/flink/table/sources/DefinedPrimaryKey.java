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

import javax.annotation.Nullable;

import java.util.List;

/**
 * The {@link DefinedPrimaryKey} interface can extends a {@link TableSource} to specify the
 * primary key meta information.
 *
 * <p>A primary key is a column or a group of columns that uniquely identifies each row in
 * a table or stream.
 *
 * <p>NOTE: Although a primary key usually has an Unique Index, if you have defined
 * a primary key, there is no need to define a same index in {@link DefinedIndexes} again.
 */
public interface DefinedPrimaryKey {

	/**
	 * Returns the column names of the primary key. Returns null if no primary key existed
	 * in the {@link TableSource}.
	 */
	@Nullable
	List<String> getPrimaryKeyColumns();

}
