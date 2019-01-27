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

package org.apache.flink.table.tpc

import java.util

import com.google.common.collect.ImmutableSet
import org.apache.flink.table.api.types.{DataTypes, InternalType}


object Customer extends TpchSchema {
  def getFieldNames: Array[String] = {
    Array(
      "c_custkey",
      "c_name",
      "c_address",
      "c_nationkey",
      "c_phone",
      "c_acctbal",
      "c_mktsegment",
      "c_comment"
    )
  }

  def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.DOUBLE,
      DataTypes.STRING,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("c_custkey"))

}

object Lineitem extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "l_orderkey",
      "l_partkey",
      "l_suppkey",
      "l_linenumber",
      "l_quantity",
      "l_extendedprice",
      "l_discount",
      "l_tax",
      "l_returnflag",
      "l_linestatus",
      "l_shipdate",
      "l_commitdate",
      "l_receiptdate",
      "l_shipinstruct",
      "l_shipmode",
      "l_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.LONG,
      DataTypes.LONG,
      DataTypes.INT,
      DataTypes.DOUBLE,
      DataTypes.DOUBLE,
      DataTypes.DOUBLE,
      DataTypes.DOUBLE,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.DATE,
      DataTypes.DATE,
      DataTypes.DATE,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("l_orderkey", "l_linenumber"))
}

object Nation extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "n_nationkey",
      "n_name",
      "n_regionkey",
      "n_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.LONG,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("n_nationkey"))
}

object Order extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "o_orderkey",
      "o_custkey",
      "o_orderstatus",
      "o_totalprice",
      "o_orderdate",
      "o_orderpriority",
      "o_clerk",
      "o_shippriority",
      "o_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.DOUBLE,
      DataTypes.DATE,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.INT,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("o_orderkey"))
}

object Part extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "p_partkey",
      "p_name",
      "p_mfgr",
      "p_brand",
      "p_type",
      "p_size",
      "p_container",
      "p_retailprice",
      "p_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.INT,
      DataTypes.STRING,
      DataTypes.DOUBLE,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("p_partkey"))
}

object Partsupp extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "ps_partkey",
      "ps_suppkey",
      "ps_availqty",
      "ps_supplycost",
      "ps_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.LONG,
      DataTypes.INT,
      DataTypes.DOUBLE,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("ps_partkey", "ps_suppkey"))
}

object Region extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "r_regionkey",
      "r_name",
      "r_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("r_regionkey"))
}

object Supplier extends TpchSchema {
  override def getFieldNames: Array[String] = {
    Array(
      "s_suppkey",
      "s_name",
      "s_address",
      "s_nationkey",
      "s_phone",
      "s_acctbal",
      "s_comment"
    )
  }

  override def getFieldTypes: Array[InternalType] = {
    Array(
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.LONG,
      DataTypes.STRING,
      DataTypes.DOUBLE,
      DataTypes.STRING
    )
  }

  override def getUniqueKeys: util.Set[util.Set[String]] =
    ImmutableSet.of(ImmutableSet.of("s_suppkey"))
}

object TpcHSchemaProvider {
  val schemaMap: Map[String, Schema] = Map(
    "customer" -> Customer,
    "lineitem" -> Lineitem,
    "nation" -> Nation,
    "orders" -> Order,
    "part" -> Part,
    "partsupp" -> Partsupp,
    "region" -> Region,
    "supplier" -> Supplier
  )

  def getSchema(tableName: String): Schema = {
    if (schemaMap.contains(tableName)) {
      schemaMap(tableName)
    } else {
      throw new IllegalArgumentException(s"$tableName does not exist!")
    }
  }
}
