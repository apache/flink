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

import org.apache.flink.table.catalog.TableSourceConverter
import org.apache.flink.table.descriptors.DescriptorProperties

/**
  * Exception for all errors occurring during expression parsing.
  */
case class ExpressionParserException(msg: String) extends RuntimeException(msg)

/**
  * Exception for all errors occurring during sql parsing.
  */
case class SqlParserException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

/**
  * General Exception for all errors during table handling.
  *
  * This exception indicates that an internal error occurred or that a feature is not supported
  * yet. Usually, this exception does not indicate a fault of the user.
  */
case class TableException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

object TableException {
  def apply(msg: String): TableException = new TableException(msg)
}

/**
  * Exception for all errors occurring during validation phase.
  *
  * This exception indicates that the user did something wrong.
  */
case class ValidationException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

object ValidationException {
  def apply(msg: String): ValidationException = new ValidationException(msg)
}

/**
  * Exception for unwanted method calling on unresolved expression.
  */
case class UnresolvedException(msg: String) extends RuntimeException(msg)

/**
  * Exception for an operation on a nonexistent table
  *
  * @param catalog    catalog name
  * @param table      table name
  * @param cause      the cause
  */
case class TableNotExistException(
    catalog: String,
    table: String,
    cause: Throwable)
    extends RuntimeException(s"Table $catalog.$table does not exist.", cause) {

  def this(catalog: String, table: String) = this(catalog, table, null)

}

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
  * Exception for operation on a nonexistent catalog
  *
  * @param catalog catalog name
  * @param cause the cause
  */
case class CatalogNotExistException(
    catalog: String,
    cause: Throwable)
    extends RuntimeException(s"Catalog $catalog does not exist.", cause) {

  def this(catalog: String) = this(catalog, null)
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
  * Exception for not finding a [[org.apache.flink.table.sources.TableSourceFactory]] for the
  * given properties.
  *
  * @param properties properties that describe the table source
  * @param cause the cause
  */
case class NoMatchingTableSourceException(
    properties: Map[String, String],
    cause: Throwable)
    extends RuntimeException(
      s"Could not find a table source factory in the classpath satisfying the " +
        s"following properties: \n" +
        s"${DescriptorProperties.toString(properties)}",
      cause) {

  def this(properties: Map[String, String]) = this(properties, null)
}

/**
  * Exception for finding more than one [[org.apache.flink.table.sources.TableSourceFactory]] for
  * the given properties.
  *
  * @param properties properties that describe the table source
  * @param cause the cause
  */
case class AmbiguousTableSourceException(
    properties: Map[String, String],
    cause: Throwable)
    extends RuntimeException(
      s"More than one table source factory in the classpath satisfying the " +
        s"following properties: \n" +
        s"${DescriptorProperties.toString(properties)}",
      cause) {

  def this(properties: Map[String, String]) = this(properties, null)
}

/**
  * Exception for not finding a [[TableSourceConverter]] for a given table type.
  *
  * @param tableType table type
  * @param cause the cause
  * @deprecated Use table source factories instead
  *            (see [[org.apache.flink.table.sources.TableSourceFactory]]).
  */
@Deprecated
@deprecated("Use table factories (see TableSourceFactory) instead.")
case class NoMatchedTableSourceConverterException(
    tableType: String,
    cause: Throwable)
    extends RuntimeException(s"Could not find a TableSourceConverter for table type $tableType.",
      cause) {

  def this(tableType: String) = this(tableType, null)
}

/**
  * Exception for finding more than one [[TableSourceConverter]] for a given table type.
  *
  * @param tableType table type
  * @param cause the cause
  * @deprecated Use table source factories instead
  *            (see [[org.apache.flink.table.sources.TableSourceFactory]]).
  */
@Deprecated
@deprecated("Use table factories (see TableSourceFactory) instead.")
case class AmbiguousTableSourceConverterException(
    tableType: String,
    cause: Throwable)
    extends RuntimeException(s"More than one TableSourceConverter for table type $tableType.",
      cause) {

  def this(tableType: String) = this(tableType, null)
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
