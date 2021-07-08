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

package org.apache.flink.table.planner.delegation.hive.desc;

import org.apache.hadoop.hive.ql.plan.PrincipalDesc;

import java.io.Serializable;
import java.util.Map;

/** Desc for alter database. */
public class HiveParserAlterDatabaseDesc implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Type of the alter db operation. */
    public enum AlterDBType {
        ALTER_PROPERTY,
        ALTER_OWNER,
        ALTER_LOCATION
    }

    private final AlterDBType alterType;
    private final String databaseName;
    private final Map<String, String> dbProperties;
    private final PrincipalDesc ownerPrincipal;
    private final String location;

    private HiveParserAlterDatabaseDesc(
            AlterDBType alterType,
            String databaseName,
            Map<String, String> dbProperties,
            PrincipalDesc ownerPrincipal,
            String location) {
        this.alterType = alterType;
        this.databaseName = databaseName;
        this.dbProperties = dbProperties;
        this.ownerPrincipal = ownerPrincipal;
        this.location = location;
    }

    public AlterDBType getAlterType() {
        return alterType;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Map<String, String> getDbProperties() {
        return dbProperties;
    }

    public PrincipalDesc getOwnerPrincipal() {
        return ownerPrincipal;
    }

    public String getLocation() {
        return location;
    }

    public static HiveParserAlterDatabaseDesc alterProps(
            String databaseName, Map<String, String> dbProperties) {
        return new Builder(AlterDBType.ALTER_PROPERTY, databaseName).newProps(dbProperties).build();
    }

    public static HiveParserAlterDatabaseDesc alterOwner(
            String databaseName, PrincipalDesc ownerPrincipal) {
        return new Builder(AlterDBType.ALTER_OWNER, databaseName).newOwner(ownerPrincipal).build();
    }

    public static HiveParserAlterDatabaseDesc alterLocation(String databaseName, String location) {
        return new Builder(AlterDBType.ALTER_LOCATION, databaseName).newLocation(location).build();
    }

    private static class Builder {
        private final AlterDBType alterType;
        private final String databaseName;
        private Map<String, String> dbProperties;
        private PrincipalDesc ownerPrincipal;
        private String location;

        Builder(AlterDBType alterType, String databaseName) {
            this.alterType = alterType;
            this.databaseName = databaseName;
        }

        Builder newProps(Map<String, String> dbProperties) {
            this.dbProperties = dbProperties;
            return this;
        }

        Builder newOwner(PrincipalDesc ownerPrincipal) {
            this.ownerPrincipal = ownerPrincipal;
            return this;
        }

        Builder newLocation(String location) {
            this.location = location;
            return this;
        }

        HiveParserAlterDatabaseDesc build() {
            return new HiveParserAlterDatabaseDesc(
                    alterType, databaseName, dbProperties, ownerPrincipal, location);
        }
    }
}
