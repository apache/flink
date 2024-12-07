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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.api.TableEnvironment;

import javax.annotation.Nullable;

/** Table environment execution related utilities. */
public final class TableEnvExecutorUtil {

    public static void executeInSeparateDatabase(
            TableEnvironment tEnv, boolean useDb, Callback execOp) throws Exception {
        executeInSeparateDatabase(tEnv, useDb, execOp, null);
    }

    /**
     * Creates a separate database "db1", calls any operation given in {@code execOp}, then cleans
     * up the created database.
     *
     * @param tEnv table environment to create the database in
     * @param useDb if true, call {@link TableEnvironment#useDatabase(String)} for the created
     *     database
     * @param execOp any operation to execute after the database creation
     * @param cleanupOp custom cleanup operations after every operation is executed
     * @throws Exception
     */
    public static void executeInSeparateDatabase(
            TableEnvironment tEnv, boolean useDb, Callback execOp, @Nullable Callback cleanupOp)
            throws Exception {
        String originalDb = tEnv.getCurrentDatabase();
        tEnv.executeSql("create database db1");
        if (useDb) {
            tEnv.useDatabase("db1");
        }
        try {
            execOp.call();
        } finally {
            if (useDb) {
                tEnv.useDatabase(originalDb);
            }
            tEnv.executeSql("drop database db1 cascade");
            if (cleanupOp != null) {
                cleanupOp.call();
            }
        }
    }

    /** Functional interface to handle operations in a callable manner. */
    public interface Callback {

        void call() throws Exception;
    }
}
