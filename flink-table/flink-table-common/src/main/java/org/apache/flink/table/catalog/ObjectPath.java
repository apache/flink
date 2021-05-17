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

package org.apache.flink.table.catalog;

import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A database name and object (table/view/function) name combo in a catalog. */
public class ObjectPath implements Serializable {
    private final String databaseName;
    private final String objectName;

    public ObjectPath(String databaseName, String objectName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(objectName),
                "objectName cannot be null or empty");

        this.databaseName = databaseName;
        this.objectName = objectName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getFullName() {
        return String.format("%s.%s", databaseName, objectName);
    }

    public static ObjectPath fromString(String fullName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

        String[] paths = fullName.split("\\.");

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get split '%s' to get databaseName and objectName", fullName));
        }

        return new ObjectPath(paths[0], paths[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectPath that = (ObjectPath) o;

        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(objectName, that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, objectName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s", databaseName, objectName);
    }
}
