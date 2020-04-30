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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;

/**
 * Implementation of {@link DataTypeVisitor} that redirects all calls to
 * {@link DataTypeDefaultVisitor#defaultMethod(DataType)}.
 */
@Internal
public abstract class DataTypeDefaultVisitor<R> implements DataTypeVisitor<R>{

	@Override
	public R visit(AtomicDataType atomicDataType) {
		return defaultMethod(atomicDataType);
	}

	@Override
	public R visit(CollectionDataType collectionDataType) {
		return defaultMethod(collectionDataType);
	}

	@Override
	public R visit(FieldsDataType fieldsDataType) {
		return defaultMethod(fieldsDataType);
	}

	@Override
	public R visit(KeyValueDataType keyValueDataType) {
		return defaultMethod(keyValueDataType);
	}

	protected abstract R defaultMethod(DataType dataType);
}
