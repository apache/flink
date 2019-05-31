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

package org.apache.flink.table.catalog.hive.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import java.util.List;

/**
 * A shim layer to support different versions of HMS.
 */
public interface HiveShim {

	/**
	 * Create a Hive Metastore client based on the given HiveConf object.
	 *
	 * @param hiveConf HiveConf instance
	 * @return an IMetaStoreClient instance
	 */
	IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf);

	/**
	 * Get a list of views in the given database from the given Hive Metastore client.
	 *
	 * @param client       Hive Metastore client
	 * @param databaseName the name of the database
	 * @return A list of names of the views
	 * @throws UnknownDBException if the database doesn't exist
	 * @throws TException         for any other generic exceptions caused by Thrift
	 */
	List<String> getViews(IMetaStoreClient client, String databaseName) throws UnknownDBException, TException;

	/**
	 * Gets a function from a database with the given HMS client.
	 *
	 * @param client       the Hive Metastore client
	 * @param dbName       name of the database
	 * @param functionName name of the function
	 * @return the Function under the specified name
	 * @throws NoSuchObjectException if the function doesn't exist
	 * @throws TException            for any other generic exceptions caused by Thrift
	 */
	Function getFunction(IMetaStoreClient client, String dbName, String functionName) throws NoSuchObjectException, TException;
}
