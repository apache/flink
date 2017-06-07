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
package org.apache.flink.api.table.tpch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.table.sources.CsvTableSource
import org.apache.flink.api.table.{TableEnvironment, Types}
import org.junit.{Before, Ignore, Test}

class TpchFunctionalityTest {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val tEnv = TableEnvironment.getTableEnvironment(env)

  @Before
  def setup(): Unit = {

    val part = new CsvTableSource(
      getClass.getResource("/tpch/data/part/part.tbl").getFile,
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
      ),
      Array(
        Types.LONG,
        Types.STRING,
        Types.STRING,
        Types.STRING,
        Types.STRING,
        Types.INT,
        Types.STRING,
        Types.DOUBLE,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val supplier = new CsvTableSource(
      getClass.getResource("/tpch/data/supplier/supplier.tbl").getFile,
      Array(
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment"
      ),
      Array(
        Types.LONG,
        Types.STRING,
        Types.STRING,
        Types.LONG,
        Types.STRING,
        Types.DOUBLE,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val partsupp = new CsvTableSource(
      getClass.getResource("/tpch/data/partsupp/partsupp.tbl").getFile,
      Array(
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment"
      ),
      Array(
        Types.LONG,
        Types.LONG,
        Types.INT,
        Types.DOUBLE,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val customer = new CsvTableSource(
      getClass.getResource("/tpch/data/customer/customer.tbl").getFile,
      Array(
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment"
      ),
      Array(
        Types.LONG,
        Types.STRING,
        Types.STRING,
        Types.LONG,
        Types.STRING,
        Types.DOUBLE,
        Types.STRING,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val orders = new CsvTableSource(
      getClass.getResource("/tpch/data/orders/orders.tbl").getFile,
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
      ),
      Array(
        Types.LONG,
        Types.LONG,
        Types.STRING,
        Types.DOUBLE,
        Types.DATE,
        Types.STRING,
        Types.STRING,
        Types.INT,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val lineitem = new CsvTableSource(
      getClass.getResource("/tpch/data/lineitem/lineitem.tbl").getFile,
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
      ),
      Array(
        Types.LONG,
        Types.LONG,
        Types.LONG,
        Types.INT,
        Types.DOUBLE,
        Types.DOUBLE,
        Types.DOUBLE,
        Types.DOUBLE,
        Types.STRING,
        Types.STRING,
        Types.DATE,
        Types.DATE,
        Types.DATE,
        Types.STRING,
        Types.STRING,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val nation = new CsvTableSource(
      getClass.getResource("/tpch/data/nation/nation.tbl").getFile,
      Array(
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment"
      ),
      Array(
        Types.LONG,
        Types.STRING,
        Types.LONG,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    val region = new CsvTableSource(
      getClass.getResource("/tpch/data/region/region.tbl").getFile,
      Array(
        "r_regionkey",
        "r_name",
        "r_comment"
      ),
      Array(
        Types.LONG,
        Types.STRING,
        Types.STRING
      ),
      fieldDelim = "|",
      rowDelim = "\n"
    )

    tEnv.registerTableSource("part", part)
    tEnv.registerTableSource("supplier", supplier)
    tEnv.registerTableSource("partsupp", partsupp)
    tEnv.registerTableSource("customer", customer)
    tEnv.registerTableSource("orders", orders)
    tEnv.registerTableSource("lineitem", lineitem)
    tEnv.registerTableSource("nation", nation)
    tEnv.registerTableSource("region", region)

  }

  @Test
  def testTpch01(): Unit = {
    val query = "01.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch02(): Unit = {
    val query = "02.sql"
    doExplain(query)
  }

  @Test
  def testTpch03(): Unit = {
    val query = "03.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch04(): Unit = {
    val query = "04.sql"
    doExplain(query)
  }

  @Test
  def testTpch05(): Unit = {
    val query = "05.sql"
    doExplain(query)
  }

  @Test
  def testTpch06(): Unit = {
    val query = "06.sql"
    doExplain(query)
  }

  @Test
  def testTpch07(): Unit = {
    val query = "07.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch08(): Unit = {
    val query = "08.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch09(): Unit = {
    val query = "09.sql"
    doExplain(query)
  }

  @Test
  def testTpch10(): Unit = {
    val query = "10.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch11(): Unit = {
    val query = "11.sql"
    doExplain(query)
  }

  @Test
  def testTpch12(): Unit = {
    val query = "12.sql"
    doExplain(query)
  }

  @Test
  def testTpch13(): Unit = {
    val query = "13.sql"
    doExplain(query)
  }

  @Test
  def testTpch14(): Unit = {
    val query = "14.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch15(): Unit = {
    val query = "15.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch16(): Unit = {
    val query = "16.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch17(): Unit = {
    val query = "17.sql"
    doExplain(query)
  }

  @Test
  def testTpch18(): Unit = {
    val query = "18.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch19(): Unit = {
    val query = "19.sql"
    doExplain(query)
  }

  @Test
  def testTpch19_1(): Unit = {
    val query = "19_1.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch20(): Unit = {
    val query = "20.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch21(): Unit = {
    val query = "21.sql"
    doExplain(query)
  }

  @Ignore
  @Test
  def testTpch22(): Unit = {
    val query = "22.sql"
    doExplain(query)
  }

  private def doExplain(query: String): Unit = {
    println(s"Begin to explain $query")

    val queryString = scala.io.Source.fromFile(
      getClass.getResource(s"/tpch/queries/$query").getFile).mkString
    val resultTable = tEnv.sql(queryString)

    println(tEnv.explain(resultTable))
  }

}
