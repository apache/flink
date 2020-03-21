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

package org.apache.flink.table.utils

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.factories.{TableFactoryUtil, TableSinkFactory, TableSourceFactory}
import org.apache.flink.table.utils.TestContextTableFactory.REQUIRED_KEY
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

import org.junit.Assert

import java.{lang, util}

/**
  * Test [[TableSourceFactory]] and [[TableSinkFactory]] for context.
  */
class TestContextTableFactory[T](
    sourceIdentifier: ObjectIdentifier,
    sinkIdentifier: ObjectIdentifier)
    extends TableSourceFactory[T] with TableSinkFactory[T] {

  var hasInvokedSource = false
  var hasInvokedSink = false

  override def requiredContext(): util.Map[String, String] = {
    throw new UnsupportedOperationException
  }

  override def supportedProperties(): util.List[String] = {
    throw new UnsupportedOperationException
  }

  override def createTableSource(context: TableSourceFactory.Context): TableSource[T] = {
    Assert.assertTrue(context.getConfiguration.get(REQUIRED_KEY))
    Assert.assertEquals(sourceIdentifier, context.getObjectIdentifier)
    hasInvokedSource = true
    TableFactoryUtil.findAndCreateTableSource(context)
  }

  override def createTableSink(context: TableSinkFactory.Context): TableSink[T] = {
    Assert.assertTrue(context.getConfiguration.get(REQUIRED_KEY))
    Assert.assertEquals(sinkIdentifier, context.getObjectIdentifier)
    hasInvokedSink = true
    TableFactoryUtil.findAndCreateTableSink(context)
  }
}

object TestContextTableFactory{
  val REQUIRED_KEY: ConfigOption[lang.Boolean] = ConfigOptions
      .key("testing.required.key").booleanType().defaultValue(false)
}
