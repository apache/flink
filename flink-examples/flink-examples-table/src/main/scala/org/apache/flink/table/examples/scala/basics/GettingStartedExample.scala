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

package org.apache.flink.table.examples.scala.basics

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, _}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.apache.flink.util.CloseableIterator

import java.time.LocalDate

import scala.collection.JavaConverters._

/**
 * Example for getting started with the Table & SQL API in Scala.
 *
 * The example shows how to create, transform, and query a table. It should give a first impression
 * about the look-and-feel of the API without going too much into details. See the other examples for
 * using connectors or more complex operations.
 *
 * In particular, the example shows how to
 *   - setup a [[TableEnvironment]],
 *   - use the environment for creating example tables, registering views, and executing SQL queries,
 *   - transform tables with filters and projections,
 *   - declare user-defined functions,
 *   - and print/collect results locally.
 *
 * The example executes two Flink jobs. The results are written to stdout.
 */
object GettingStartedExample {

  def main(args: Array[String]): Unit = {

    // setup the unified API
    // in this case: declare that the table programs should be executed in batch mode
    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .build()
    val env = TableEnvironment.create(settings)

    // create a table with example data without a connector required
    val rawCustomers = env.fromValues(
      row("Guillermo Smith", LocalDate.parse("1992-12-12"), "4081 Valley Road", "08540", "New Jersey", "m", true, 0, 78, 3),
      row("Valeria Mendoza", LocalDate.parse("1970-03-28"), "1239  Rainbow Road", "90017", "Los Angeles", "f", true, 9, 39, 0),
      row("Leann Holloway", LocalDate.parse("1989-05-21"), "2359 New Street", "97401", "Eugene", null, true, null, null, null),
      row("Brandy Sanders", LocalDate.parse("1956-05-26"), "4891 Walkers-Ridge-Way", "73119", "Oklahoma City", "m", false, 9, 39, 0),
      row("John Turner", LocalDate.parse("1982-10-02"), "2359 New Street", "60605", "Chicago", "m", true, 12, 39, 0),
      row("Ellen Ortega", LocalDate.parse("1985-06-18"), "2448 Rodney STreet", "85023", "Phoenix", "f", true, 0, 78, 3)
    )

    // handle ranges of columns easily
    val truncatedCustomers = rawCustomers.select(withColumns(1 to 7))

    // name columns
    val namedCustomers = truncatedCustomers
      .as("name", "date_of_birth", "street", "zip_code", "city", "gender", "has_newsletter")

    // register a view temporarily
    env.createTemporaryView("customers", namedCustomers)

    // use SQL whenever you like
    // call execute() and print() to get insights
    env
      .sqlQuery("""
        |SELECT
        |  COUNT(*) AS `number of customers`,
        |  AVG(YEAR(date_of_birth)) AS `average birth year`
        |FROM `customers`
        |""".stripMargin
      )
      .execute()
      .print()

    // or further transform the data using the fluent Table API
    // e.g. filter, project fields, or call a user-defined function
    val youngCustomers = env
      .from("customers")
      .filter($"gender".isNotNull)
      .filter($"has_newsletter" === true)
      .filter($"date_of_birth" >= LocalDate.parse("1980-01-01"))
      .select(
        $"name".upperCase(),
        $"date_of_birth",
        call(classOf[AddressNormalizer], $"street", $"zip_code", $"city").as("address")
      )

    // use execute() and collect() to retrieve your results from the cluster
    // this can be useful for testing before storing it in an external system
    var iterator: CloseableIterator[Row] = null
    try {
      iterator = youngCustomers.execute().collect()
      val actualOutput = iterator.asScala.toSet

      val expectedOutput = Set(
        Row.of("GUILLERMO SMITH", LocalDate.parse("1992-12-12"), "4081 VALLEY ROAD, 08540, NEW JERSEY"),
        Row.of("JOHN TURNER", LocalDate.parse("1982-10-02"), "2359 NEW STREET, 60605, CHICAGO"),
        Row.of("ELLEN ORTEGA", LocalDate.parse("1985-06-18"), "2448 RODNEY STREET, 85023, PHOENIX")
      )

      if (actualOutput == expectedOutput) {
        println("SUCCESS!")
      } else {
        println("FAILURE!")
      }
    } finally {
      if (iterator != null) {
        iterator.close()
      }
    }
  }

  /**
   * We can put frequently used procedures in user-defined functions.
   *
   * It is possible to call third-party libraries here as well.
   */
  class AddressNormalizer extends ScalarFunction {

    // the 'eval()' method defines input and output types (reflectively extracted)
    // and contains the runtime logic
    def eval(street: String, zipCode: String, city: String): String = {
      normalize(street) + ", " + normalize(zipCode) + ", " + normalize(city)
    }

    private def normalize(s: String) = {
      s.toUpperCase.replaceAll("\\W", " ").replaceAll("\\s+", " ").trim
    }
  }
}
