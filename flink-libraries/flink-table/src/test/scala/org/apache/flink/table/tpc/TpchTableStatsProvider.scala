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

import java.sql.Date

import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.tpc.STATS_MODE.STATS_MODE

import scala.collection.JavaConversions._

object TpchTableStatsProvider {
  // factor 1
  val CUSTOMER_1 = TableStats(150000L)
  val LINEITEM_1 = TableStats(6000000L)
  val NATION_1 = TableStats(25L)
  val ORDER_1 = TableStats(1500000L)
  val PART_1 = TableStats(20000L)
  val PARTSUPP_1 = TableStats(800000L)
  val REGION_1 = TableStats(5L)
  val SUPPLIER_1 = TableStats(10000L)

  // factor 1000
  val CUSTOMER_1000 = TableStats(150000000L, Map[String, ColumnStats](
    "c_acctbal" -> ColumnStats(1098928L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "c_name" -> ColumnStats(73206L, 0L, 18.0D, 18, "Customer#150000000", "Customer#000000001"),
    "c_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "c_custkey" -> ColumnStats(151705138L, 0L, 8.0D, 8, 150000000L, 1L),
    "c_comment" -> ColumnStats(116753779L, 0L, 72.49981046D, 116,
      "zzle? special accounts about the iro",
      " Tiresias about the accounts haggle quiet, busy foxe"),
    "c_address" -> ColumnStats(100916636L, 0L, 24.999506266666668D, 40, "zzzzyW,aeC8HnFV",
      "    2WGW,hiM7jHg2"),
    "c_mktsegment" -> ColumnStats(5L, 0L, 8.999981466666666D, 10, "MACHINERY", "AUTOMOBILE"),
    "c_phone" -> ColumnStats(979840L, 0L, 15.0D, 15, "34-999-999-9215", "10-100-100-3024")
  ))

  val LINEITEM_1000 = TableStats(5999989709L, Map[String, ColumnStats](
    "l_returnflag" -> ColumnStats(3L, 0L, 1.0D, 1, "R", "A"),
    "l_comment" -> ColumnStats(97099241L, 0L, 26.499693244557196D, 43, "zzle? unusual",
      " Tiresias "),
    "l_linestatus" -> ColumnStats(2L, 0L, 1.0D, 1, "O", "F"),
    "l_shipmode" -> ColumnStats(7L, 0L, 4.285721156726071D, 7, "TRUCK", "AIR"),
    "l_shipinstruct" -> ColumnStats(4L, 0L, 11.99999560182579D, 17, "TAKE BACK RETURN",
      "COLLECT COD"),
    "l_quantity" -> ColumnStats(50L, 0L, 8.0D, 8, 50.0D, 1.0D),
    "l_receiptdate" -> ColumnStats(2556L, 0L, 12.0D, 12, Date.valueOf("1998-12-30"),
      Date.valueOf("1992-01-02")),
    "l_linenumber" -> ColumnStats(7L, 0L, 4.0D, 4, 7, 1),
    "l_tax" -> ColumnStats(9L, 0L, 8.0D, 8, 0.08D, 0.0D),
    "l_shipdate" -> ColumnStats(2524L, 0L, 12.0D, 12, Date.valueOf("1998-11-30"),
      Date.valueOf("1992-01-01")),
    "l_extendedprice" -> ColumnStats(3818583L, 0L, 8.0D, 8, 104950.0D, 900.0D),
    "l_partkey" -> ColumnStats(201326292L, 0L, 8.0D, 8, 200000000L, 1L),
    "l_discount" -> ColumnStats(11L, 0L, 8.0D, 8, 0.1D, 0.0D),
    "l_commitdate" -> ColumnStats(2468L, 0L, 12.0D, 12, Date.valueOf("1998-10-30"),
      Date.valueOf("1992-01-30")),
    "l_suppkey" -> ColumnStats(9921233L, 0L, 8.0D, 8, 10000000L, 1L),
    "l_orderkey" -> ColumnStats(1490108544L, 0L, 8.0D, 8, 6000000000L, 1L)
  ))

  val NATION_1000 = TableStats(25L, Map[String, ColumnStats](
    "n_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "n_name" -> ColumnStats(24L, 0L, 7.08D, 14, "VIETNAM", "ALGERIA"),
    "n_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "n_comment" -> ColumnStats(25L, 0L, 74.28D, 114,
      "y final packages. slow foxes cajole quickly. quickly silent platelets breach " +
          "ironic accounts. unusual pinto be",
      " haggle. carefully final deposits detect slyly agai")
  ))

  val ORDER_1000 = TableStats(1500000000L, Map[String, ColumnStats](
    "o_shippriority" -> ColumnStats(1L, 0L, 4.0D, 4, 0, 0),
    "o_orderdate" -> ColumnStats(2408L, 0L, 12.0D, 12, Date.valueOf("1998-08-01"),
      Date.valueOf("1991-12-31")),
    "o_custkey" -> ColumnStats(100594195L, 0L, 8.0D, 8, 149999999L, 1L),
    "o_orderpriority" -> ColumnStats(5L, 0L, 8.399956616D, 15, "5-LOW", "1-URGENT"),
    "o_clerk" -> ColumnStats(1515757L, 0L, 15.0D, 15, "Clerk#001000000", "Clerk#000000001"),
    "o_orderstatus" -> ColumnStats(3L, 0L, 1.0D, 1, "P", "F"),
    "o_totalprice" -> ColumnStats(41853126L, 0L, 8.0D, 8, 602901.81D, 810.87D),
    "o_orderkey" -> ColumnStats(1490108544L, 0L, 8.0D, 8, 6000000000L, 1L),
    "o_comment" -> ColumnStats(233890521L, 0L, 48.499603414666666D, 78, "zzle? unusual requests w",
      " Tiresias about the")
  ))

  val PART_1000 = TableStats(200000000L, Map[String, ColumnStats](
    "p_comment" -> ColumnStats(4494570L, 0L, 13.4999255D, 22, "zzle? speci", " Tire"),
    "p_name" -> ColumnStats(125980008L, 0L, 33.117700585D, 52,
      "yellow white wheat turquoise cornflower", "almond antique aquamarine azure bisque"),
    "p_type" -> ColumnStats(150L, 0L, 20.59989332D, 25, "STANDARD POLISHED TIN",
      "ECONOMY ANODIZED BRASS"),
    "p_retailprice" -> ColumnStats(119478L, 0L, 8.0D, 8, 2099.0D, 900.0D),
    "p_mfgr" -> ColumnStats(5L, 0L, 14.0D, 14, "Manufacturer#5", "Manufacturer#1"),
    "p_container" -> ColumnStats(40L, 0L, 7.5750465D, 10, "WRAP PKG", "JUMBO BAG"),
    "p_brand" -> ColumnStats(25L, 0L, 8.0D, 8, "Brand#55", "Brand#11"),
    "p_size" -> ColumnStats(50L, 0L, 4.0D, 4, 50, 1),
    "p_partkey" -> ColumnStats(201326292L, 0L, 8.0D, 8, 200000000L, 1L)
  ))

  val PARTSUPP_1000 = TableStats(800000000L, Map[String, ColumnStats](
    "ps_availqty" -> ColumnStats(10048L, 0L, 4.0D, 4, 9999, 1),
    "ps_comment" -> ColumnStats(296273021L, 0L, 123.4988338675D, 198,
      "zzle? unusual requests wake slyly. slyly regular requests are e",
      " Tiresias about the accounts detect quickly final foxes. " +
          "instructions about the blithely unusual theodolites use blithely f"),
    "ps_partkey" -> ColumnStats(201326292L, 0L, 8.0D, 8, 200000000L, 1L),
    "ps_supplycost" -> ColumnStats(101492L, 0L, 8.0D, 8, 1000.0D, 1.0D),
    "ps_suppkey" -> ColumnStats(9921233L, 0L, 8.0D, 8, 10000000L, 1L)
  ))

  val REGION_1000 = TableStats(5L, Map[String, ColumnStats](
    "r_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "r_name" -> ColumnStats(5L, 0L, 6.8D, 11, "MIDDLE EAST", "AFRICA"),
    "r_comment" -> ColumnStats(5L, 0L, 66.0D, 115,
      "uickly special accounts cajole carefully blithely close requests. " +
          "carefully final asymptotes haggle furiousl",
      "ges. thinly even pinto beans ca")
  ))
  val SUPPLIER_1000 = TableStats(10000000L, Map[String, ColumnStats](
    "s_phone" -> ColumnStats(913587L, 0L, 15.0D, 15, "34-999-999-3239", "10-100-101-9215"),
    "s_address" -> ColumnStats(7289833L, 0L, 25.0016709D, 40, "zzzzr MaemffsKy",
      "   04SJW3NWgeWBx2YualVtK62DXnr"),
    "s_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "s_suppkey" -> ColumnStats(9921233L, 0L, 8.0D, 8, 10000000L, 1L),
    "s_name" -> ColumnStats(4997L, 0L, 18.0D, 18, "Supplier#010000000", "Supplier#000000001"),
    "s_acctbal" -> ColumnStats(1098928L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "s_comment" -> ColumnStats(8871240L, 0L, 62.4862325D, 100,
      "zzle? special packages haggle carefully regular inst",
      " Customer  accounts are blithely furiousRecommends")
  ))

  val REGION_10240 = TableStats(5L, Map[String, ColumnStats](
    "r_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "r_name" -> ColumnStats(5L, 0L, 6.8D, 11, "MIDDLE EAST", "AFRICA"),
    "r_comment" -> ColumnStats(5L, 0L, 66.0D, 115,
      "uickly special accounts cajole carefully blithely close requests." +
          " carefully final asymptotes haggle furiousl", "ges. thinly even pinto beans ca")))

  val NATION_10240 = TableStats(25L, Map[String, ColumnStats](
    "n_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "n_name" -> ColumnStats(24L, 0L, 7.08D, 14, "VIETNAM", "ALGERIA"),
    "n_regionkey" -> ColumnStats(5L, 0L, 8.0D, 8, 4L, 0L),
    "n_comment" -> ColumnStats(25L, 0L, 74.28D, 114, "y final packages. " +
        "slow foxes cajole quickly. quickly silent platelets breach ironic accounts." +
        " unusual pinto be", " haggle. carefully final deposits detect slyly agai")))

  val SUPPLIER_10240 = TableStats(102400000L, Map[String, ColumnStats](
    "s_phone" -> ColumnStats(960780L, 0L, 15.0D, 15, "34-999-999-7717", "10-100-100-4408"),
    "s_address" -> ColumnStats(69368473L, 0L, 24.999831376953125D, 40,
      "zzzzwxT8ep iLZf86nWDfH6JXrgavK", "    9iVW,ybe3e44LX"),
    "s_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "s_suppkey" -> ColumnStats(103573589L, 0L, 8.0D, 8, 102400000L, 1L),
    "s_name" -> ColumnStats(50636L, 0L, 18.0D, 18, "Supplier#102400000",
      "Supplier#000000001"),
    "s_acctbal" -> ColumnStats(1098928L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "s_comment" -> ColumnStats(79030284L, 0L, 62.50012087890625D, 100,
      "zzleCustomer y. even accComplaints blithely", " Customer  Complaintseven in")))

  val CUSTOMER_10240 = TableStats(1536000000L, Map[String, ColumnStats](
    "c_acctbal" -> ColumnStats(1098928L, 0L, 8.0D, 8, 9999.99D, -999.99D),
    "c_name" -> ColumnStats(755472L, 0L, 18.348958333984374D, 19, "Customer#999999999",
      "Customer#000000001"),
    "c_nationkey" -> ColumnStats(25L, 0L, 8.0D, 8, 24L, 0L),
    "c_custkey" -> ColumnStats(1530198332L, 0L, 8.0D, 8, 1536000000L, 1L),
    "c_comment" -> ColumnStats(293052413L, 0L, 72.49930418359375D, 116,
      "zzle? unusual requests wake slyly. sl",
      " Tiresias about the accounts detect quickly final foxes. instructions ab"),
    "c_address" -> ColumnStats(158915314L, 0L, 24.999929387369793D, 40,
      "zzzzyW,aeC8HnFV", "    2WGW,hiM7jHg2"),
    "c_mktsegment" -> ColumnStats(5L, 0L, 8.999996953125D, 10, "MACHINERY", "AUTOMOBILE"),
    "c_phone" -> ColumnStats(1670659L, 0L, 15.0D, 15, "34-999-999-9914", "10-100-100-1299")))

  val PART_10240 = TableStats(2048000000L, Map[String, ColumnStats](
    "p_comment" -> ColumnStats(6562438L, 0L, 13.499853077148437D, 22, "zzle? th", " Tire"),
    "p_name" -> ColumnStats(605794744L, 0L, 33.11786008642578D, 52,
      "yellow white wheat violet orchid", "almond antique aquamarine azure bisque"),
    "p_type" -> ColumnStats(150L, 0L, 20.600006720214843D, 25, "STANDARD POLISHED TIN",
      "ECONOMY ANODIZED BRASS"),
    "p_retailprice" -> ColumnStats(119478L, 0L, 8.0D, 8, 2099.0D, 900.0D),
    "p_mfgr" -> ColumnStats(5L, 0L, 14.0D, 14, "Manufacturer#5", "Manufacturer#1"),
    "p_container" -> ColumnStats(40L, 0L, 7.574998793945312D, 10, "WRAP PKG", "JUMBO BAG"),
    "p_brand" -> ColumnStats(25L, 0L, 8.0D, 8, "Brand#55", "Brand#11"),
    "p_size" -> ColumnStats(50L, 0L, 4.0D, 4, 50, 1),
    "p_partkey" -> ColumnStats(2018874731L, 0L, 8.0D, 8, 2048000000L, 1L)))

  val PARTSUPP_10240 = TableStats(8192000000L, Map[String, ColumnStats](
    "ps_availqty" -> ColumnStats(10048L, 0L, 4.0D, 4, 9999, 1),
    "ps_comment" -> ColumnStats(311179609L, 0L, 123.49857649938964D, 198,
      "zzle? unusual requests wake slyly. slyly regular requests are e",
      " Tiresias about the accounts detect quickly final foxes. " +
          "instructions about the blithely unusual theodolites use blithely f"),
    "ps_partkey" -> ColumnStats(2018874731L, 0L, 8.0D, 8, 2048000000L, 1L),
    "ps_supplycost" -> ColumnStats(101492L, 0L, 8.0D, 8, 1000.0D, 1.0D),
    "ps_suppkey" -> ColumnStats(103573589L, 0L, 8.0D, 8, 102400000L, 1L)))

  val ORDERS_10240 = TableStats(15360000000L, Map[String, ColumnStats](
    "o_shippriority" -> ColumnStats(1L, 0L, 4.0D, 4, 0, 0),
    "o_orderdate" -> ColumnStats(2408L, 0L, 12.0D, 12, Date.valueOf("1998-08-01"),
      Date.valueOf("1991-12-31")),
    "o_custkey" -> ColumnStats(1017252482L, 0L, 8.0D, 8, 1535999999L, 1L),
    "o_orderpriority" -> ColumnStats(5L, 0L, 8.399997409960937D, 15, "5-LOW", "1-URGENT"),
    "o_clerk" -> ColumnStats(15319523L, 0L, 15.0D, 15, "Clerk#010240000", "Clerk#000000001"),
    "o_orderstatus" -> ColumnStats(3L, 0L, 1.0D, 1, "P", "F"),
    "o_totalprice" -> ColumnStats(42415773L, 0L, 8.0D, 8, 597596.0D, 812.36D),
    "o_orderkey" -> ColumnStats(15359585941L, 0L, 8.0D, 8, 61440000000L, 1L),
    "o_comment" -> ColumnStats(239193840L, 0L, 48.49946462213542D, 78, "zzle? unusual requests w",
      " Tiresias about the")))

  val LINEITEM_10240 = TableStats(61440028180L, Map[String, ColumnStats](
    "l_returnflag" -> ColumnStats(3L, 0L, 1.0D, 1, "R", "A"),
    "l_comment" -> ColumnStats(109991250L, 0L, 26.499722285576595D, 43, "zzle? unusual",
      " Tiresias "),
    "l_linestatus" -> ColumnStats(2L, 0L, 1.0D, 1, "O", "F"),
    "l_shipmode" -> ColumnStats(7L, 0L, 4.285717711921792D, 7, "TRUCK", "AIR"),
    "l_shipinstruct" -> ColumnStats(4L, 0L, 11.999995248618715D, 17, "TAKE BACK RETURN",
      "COLLECT COD"),
    "l_quantity" -> ColumnStats(50L, 0L, 8.0D, 8, 50.0D, 1.0D),
    "l_receiptdate" -> ColumnStats(2556L, 0L, 12.0D, 12, Date.valueOf("1998-12-30"),
      Date.valueOf("1992-01-02")),
    "l_linenumber" -> ColumnStats(7L, 0L, 4.0D, 4, 7, 1),
    "l_tax" -> ColumnStats(9L, 0L, 8.0D, 8, 0.08D, 0.0D),
    "l_shipdate" -> ColumnStats(2524L, 0L, 12.0D, 12, Date.valueOf("1998-11-30"),
      Date.valueOf("1992-01-01")),
    "l_extendedprice" -> ColumnStats(3830758L, 0L, 8.0D, 8, 104950.0D, 900.0D),
    "l_partkey" -> ColumnStats(1891135981L, 0L, 8.0D, 8, 2048000000L, 1L),
    "l_discount" -> ColumnStats(11L, 0L, 8.0D, 8, 0.1D, 0.0D),
    "l_commitdate" -> ColumnStats(2468L, 0L, 12.0D, 12, Date.valueOf("1998-10-30"),
      Date.valueOf("1992-01-30")),
    "l_suppkey" -> ColumnStats(103573589L, 0L, 8.0D, 8, 102400000L, 1L),
    "l_orderkey" -> ColumnStats(15359585941L, 0L, 8.0D, 8, 61440000000L, 1L)))

  val statsMap: Map[String, Map[Int, TableStats]] = Map(
    "customer" -> Map(1 -> CUSTOMER_1, 1000 -> CUSTOMER_1000, 10240 -> CUSTOMER_10240),
    "lineitem" -> Map(1 -> LINEITEM_1, 1000 -> LINEITEM_1000, 10240 -> LINEITEM_10240),
    "nation" -> Map(1 -> NATION_1, 1000 -> NATION_1000, 10240 -> NATION_10240),
    "orders" -> Map(1 -> ORDER_1, 1000 -> ORDER_1000, 10240 -> ORDERS_10240),
    "part" -> Map(1 -> PART_1, 1000 -> PART_1000, 10240 -> PART_10240),
    "partsupp" -> Map(1 -> PARTSUPP_1, 1000 -> PARTSUPP_1000, 10240 -> PARTSUPP_10240),
    "region" -> Map(1 -> REGION_1, 1000 -> REGION_1000, 10240 -> REGION_10240),
    "supplier" -> Map(1 -> SUPPLIER_1, 1000 -> SUPPLIER_1000, 10240 -> SUPPLIER_10240)
  )

  def getTableStatsMap(factor: Int, statsMode: STATS_MODE): Map[String, TableStats] = {
    statsMap.map {
      case (k, v) =>
        val newTableStats = v.get(factor) match {
          case Some(tableStats) =>
            statsMode match {
              case STATS_MODE.FULL => tableStats
              case STATS_MODE.PART => getPartTableStats(tableStats)
              case STATS_MODE.ROW_COUNT => TableStats(tableStats.rowCount)
            }
          case _ => throw new IllegalArgumentException(
            s"Can not find TableStats for table:$k with factor: $factor!")
        }
        (k, newTableStats)
    }
  }

  private def getPartTableStats(tableStats: TableStats): TableStats = {
    val partColStats = tableStats.colStats.map {
      case (k, v) =>
        // Parquet metadata only includes nullCount, max, min
        val newColumnStats = ColumnStats(
          ndv = null,
          nullCount = v.nullCount,
          avgLen = null,
          maxLen = null,
          max = v.max,
          min = v.min)
        (k, newColumnStats)
    }
    TableStats(tableStats.rowCount, partColStats)
  }
}
