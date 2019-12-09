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

package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The visitor definition of {@link DataType}. The visitor transforms a data type into
 * instances of {@code R}.
 */
@PublicEvolving
public interface DataTypeVisitor<R> {

	R visit(AtomicDataType atomicDataType);

	R visit(CollectionDataType collectionDataType);

	R visit(FieldsDataType fieldsDataType);

	R visit(KeyValueDataType keyValueDataType);
}
