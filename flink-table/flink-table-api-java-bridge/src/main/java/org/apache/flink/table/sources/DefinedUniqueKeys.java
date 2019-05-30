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

import org.apache.flink.annotation.Experimental;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The {@link DefinedUniqueKeys} interface can extends a {@link TableSource} to specify the
 * unique keys meta information.
 *
 * <p>A UNIQUE KEY is a is a constraint ensures that all values in a column are different.
 * Both the UNIQUE KEY and PRIMARY KEY constraints provide a guarantee for uniqueness for
 * a column or set of columns. You can have many UNIQUE KEYs per table, but only one PRIMARY
 * KEY per table.
 *
 * <p>An unique key information will be used by optimizer and query execution.
 *
 * <p>NOTE: A PRIMARY KEY automatically has a UNIQUE KEY constraint. If you have defined a
 * PRIMARY KEY in {@link DefinedPrimaryKey}, there is no need to define a same UNIQUE KEY
 * in {@link DefinedUniqueKeys} again.
 */
@Experimental
public interface DefinedUniqueKeys {

	/**
	 * Returns the column names of the primary key. Returns null if no primary key existed
	 * in the {@link TableSource}.
	 */
	@Nullable
	List<String> getPrimaryKeyColumns();

}
