/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.rethinkdb;

/**
 * Enumeration class for conflict resolution strategy in case of id conflict between the
 * document being inserted and one existing in RethinkDB.  By default
 * RethinkDB uses "id" field as the id for the documents.
 */
public enum ConflictStrategy {
	
	/**
	 * Conflict resolution option to update the document with conflicting id
	 */
	update,
	
	/**
	 * Conflict resolution option to produce in error on conflicting id
	 */
	error,

	/**
	 * Conflict resolution option to replace the document on conflicting id
	 */
	replace;

}
