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
package org.apache.flink.table.plan

import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.api.types.{DataType, InternalType, TimestampType}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory, TableFactory}
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.util.LogicalNodeUtil
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.temptable.TableServiceOptions
import org.apache.flink.table.util.TableProperties

import _root_.scala.collection.JavaConverters._

/**
  * A helper class for rebuilding LogicalNode Plan when Table.persist/cache is enabled.
  * @param tEnv
  */
class CacheAwareRelNodePlanBuilder(tEnv: TableEnvironment) {

  /**
    * store the the mapping of original node and new node,
    * so that we won't clone twice for one node when building plan.
    */
  private val originalNewNodeMap = new java.util.IdentityHashMap[LogicalNode, LogicalNode]

  /**
    * It will rebuild the logicalNode trees if any table has been cached successfully.
    * @param sinkNodes LogicalNodes which stores in TableEnvironment.
    * @return new LogicalNodes which has been rebuilt.
    */
  def buildPlanIfNeeded(sinkNodes: Seq[LogicalNode]): Seq[LogicalNode] = {
    // clear the map, because the new created tree can also be changed again
    originalNewNodeMap.clear()

    if (tEnv.tableServiceManager.cachedTables.size() > 0) {
      buildPlan(sinkNodes)
    } else {
      sinkNodes
    }
  }

  private def buildPlan(nodes: Seq[LogicalNode]): Seq[LogicalNode] = {
    nodes.map { node =>
      // if the original node has been rebuilt/cloned, just reuse it.
      Option(originalNewNodeMap.get(node)).getOrElse {
        val tableName = tEnv.tableServiceManager.getCachedTableName(node)
        val newNode = tableName match {
          // if a child node(table) has been cached successfully, create a Source node
          case Some(name) => tEnv.scan(name).logicalPlan

          // use new children to clone a new node
          case None => LogicalNodeUtil.cloneLogicalNode(node, buildPlan(node.children))

        }
        originalNewNodeMap.put(node, newNode)
        newNode
      }
    }
  }

  private class CacheSourceSinkTableBuilder(name: String, logicalPlan: LogicalNode) {
    var tableFactory : Option[TableFactory] = tEnv.tableServiceManager.getTableServiceFactory()
    var schema: Option[RichTableSchema] = None
    var properties: Option[TableProperties] =
      tEnv.tableServiceManager.getTableServiceFactoryProperties()

    def createCacheTableSink(): TableSink[_] = {
      build()
      tableFactory.getOrElse(
        throw new Exception("TableServiceFactory is not configured")
      ) match {
        case sink: BatchTableSinkFactory[_] => {
          val tableProperties = properties.get
          tableProperties.putTableNameIntoProperties(name)
          tableProperties.putSchemaIntoProperties(schema.get)
          tableProperties.setString(
            TableServiceOptions.TABLE_SERVICE_ID,
            tEnv.tableServiceManager.getTableServiceId()
          )
          sink.createBatchTableSink(tableProperties.toMap)
        }
        case _ => throw new RuntimeException("Do not supported: " + tableFactory)
      }
    }

    def createCacheTableSource(): TableSource = {
      build()
      tableFactory.getOrElse(
        throw new Exception("TableServiceFactory is not configured")
      ) match {
        case sink: BatchTableSourceFactory[_] => {
          val tableProperties = properties.get
          tableProperties.putTableNameIntoProperties(name)
          tableProperties.putSchemaIntoProperties(schema.get)
          tableProperties.setString(
            TableServiceOptions.TABLE_SERVICE_ID,
            tEnv.tableServiceManager.getTableServiceId()
          )
          sink.createBatchTableSource(tableProperties.toMap)
        }
        case _ => throw new RuntimeException("Do not supported: " + tableFactory)
      }
    }

    private def build() = {
      val relNode = logicalPlan.toRelNode(tEnv.getRelBuilder)
      val rowType = relNode.getRowType
      val names: Array[String] = rowType.getFieldNames.asScala.toArray
      val types: Array[DataType] = rowType.getFieldList.asScala
        .map(field => FlinkTypeFactory.toInternalType(field.getType))
        .map {
          // replace time indicator types by SQL_TIMESTAMP
          case t: TimestampType if FlinkTypeFactory.isTimeIndicatorType(t)
          => TimestampType.TIMESTAMP
          case t: InternalType => t
        }.toArray

      schema = Some(new RichTableSchema(names, types.map {_.asInstanceOf[InternalType]}))
    }
  }

  /**
    * Create a Sink node for caching a table.
    * @param name A unique generated table name.
    * @param logicalPlan LogicalNode of the cached table.
    * @return TableSink[_] TableSink which the cached table will write to.
    */
  def createCacheTableSink(name: String, logicalPlan: LogicalNode): TableSink[_] = {
    val builder = new CacheSourceSinkTableBuilder(name, logicalPlan)
    builder.createCacheTableSink()
  }

  /**
    * Create a Source logical node to scan the cached table.
    * @param name A unique generated table name.
    * @param logicalPlan LogicalNode of the cached table.
    * @return TableSource TableSource of which the data will be read from.
    */
  def createCacheTableSource(name: String, logicalPlan: LogicalNode): TableSource = {
    val builder = new CacheSourceSinkTableBuilder(name, logicalPlan)
    builder.createCacheTableSource()
  }
}
