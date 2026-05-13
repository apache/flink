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
import org.apache.flink.table.catalog.ObjectPath;

/** Exception for trying to operate on a connection that doesn't exist. */
@PublicEvolving
public class ConnectionNotExistException extends Exception {

    private static final String MSG = "Connection '%s' does not exist in catalog '%s'.";
    private static final String MSG_WITHOUT_CATALOG = "Connection '%s' does not exist.";

    public ConnectionNotExistException(String catalogName, ObjectPath connectionPath) {
        this(catalogName, connectionPath, null);
    }

    public ConnectionNotExistException(
            String catalogName, ObjectPath connectionPath, Throwable cause) {
        super(formatMsg(catalogName, connectionPath), cause);
    }

    private static String formatMsg(String catalogName, ObjectPath connectionPath) {
        if (catalogName != null) {
            return String.format(MSG, connectionPath.getFullName(), catalogName);
        }
        return String.format(MSG_WITHOUT_CATALOG, connectionPath.getFullName());
    }
}
