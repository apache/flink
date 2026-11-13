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

package org.apache.flink.table.catalog.exceptions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableError;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.FlinkException;

/**
 * Exception for trying to create a table (or view) that already exists.
 *
 * <p>The exception carries {@link TableError#TABLE_OR_VIEW_ALREADY_EXISTS}.
 */
@PublicEvolving
public class TableAlreadyExistException extends FlinkException {

    private static final long serialVersionUID = 8007516829092595106L;

    public TableAlreadyExistException(String catalogName, ObjectPath tablePath) {
        this(catalogName, tablePath, null);
    }

    public TableAlreadyExistException(String catalogName, ObjectPath tablePath, Throwable cause) {
        super(
                TableError.TABLE_OR_VIEW_ALREADY_EXISTS,
                cause,
                CatalogExceptionUtils.toObjectIdentifier(catalogName, tablePath));
    }
}
