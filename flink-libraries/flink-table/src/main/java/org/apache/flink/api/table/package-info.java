/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <strong>Table API</strong><br>
 *
 * This package contains the generic part of the Table API. It can be used with Flink Streaming
 * and Flink Batch. From Scala as well as from Java.
 *
 * When using the Table API, as user creates a [[org.apache.flink.api.table.Table]] from
 * a DataSet or DataStream. On this relational operations can be performed. A table can also
 * be converted back to a DataSet or DataStream.
 *
 * Packages [[org.apache.flink.api.scala.table]] and [[org.apache.flink.api.java.table]] contain
 * the language specific part of the API. Refer to these packages for documentation on how
 * the Table API can be used in Java and Scala.
 */
package org.apache.flink.api.table;
