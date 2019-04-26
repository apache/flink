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

package org.apache.flink.table.api

/**
  * Exception for all errors occurring during expression parsing.
  */
case class ExpressionParserException(msg: String) extends RuntimeException(msg)

/**
  * Exception for unwanted method calling on unresolved expression.
  */
case class UnresolvedException(msg: String) extends RuntimeException(msg)

/**
  * Exception for adding an already existent table
  *
  * @param catalog    catalog name
  * @param table      table name
  * @param cause      the cause
  */
case class TableAlreadyExistException(
    catalog: String,
    table: String,
    cause: Throwable)
    extends RuntimeException(s"Table $catalog.$table already exists.", cause) {

  def this(catalog: String, table: String) = this(catalog, table, null)

}

/**
  * Exception for adding an already existent catalog
  *
  * @param catalog catalog name
  * @param cause the cause
  */
case class CatalogAlreadyExistException(
    catalog: String,
    cause: Throwable)
    extends RuntimeException(s"Catalog $catalog already exists.", cause) {

  def this(catalog: String) = this(catalog, null)
}

/**
  * Exception for operation on a nonexistent external catalog
  *
  * @param catalogName external catalog name
  * @param cause the cause
  */
case class ExternalCatalogNotExistException(
    catalogName: String,
    cause: Throwable)
    extends RuntimeException(s"External catalog $catalogName does not exist.", cause) {

  def this(catalogName: String) = this(catalogName, null)
}

/**
  * Exception for adding an already existent external catalog
  *
  * @param catalogName external catalog name
  * @param cause the cause
  */
case class ExternalCatalogAlreadyExistException(
    catalogName: String,
    cause: Throwable)
    extends RuntimeException(s"External catalog $catalogName already exists.", cause) {

  def this(catalogName: String) = this(catalogName, null)
}
