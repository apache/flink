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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Example for aggregating and ranking data using Flink SQL on updating tables.
 *
 * <p>The example shows how to declare a table using SQL DDL for reading bounded, insert-only data and
 * how to handle updating results in streaming mode. It should give a first impression about Flink SQL as
 * a changelog processor. The example uses some streaming operations that produce a stream of updates.
 * See the other examples for pure CDC processing and more complex operations.
 *
 * <p>In particular, the example shows how to
 * <ul>
 *     <li>setup a {@link TableEnvironment},
 *     <li>use the environment for creating a CSV file with bounded example data,
 *     <li>aggregate the incoming INSERT changes in an updating table,
 *     <li>compute an updating top-N result,
 *     <li>collect and materialize the table locally.
 * </ul>
 *
 * <p>The example executes two Flink jobs. The results are written to stdout.
 *
 * <p>Note: Make sure to include the SQL CSV format when submitting this example to Flink (e.g. via
 * command line). This step is not necessary when executing the example in an IDE.
 */
public final class UpdatingTopCityExample {

	public static void main(String[] args) throws Exception {
		// prepare the session
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.build();
		final TableEnvironment env = TableEnvironment.create(settings);

		// create an empty temporary CSV directory for this example
		final String populationDirPath = createTemporaryDirectory();

		// register a table in the catalog that points to the CSV file
		env.executeSql(
			"CREATE TABLE PopulationUpdates (" +
			"  city STRING," +
			"  state STRING," +
			"  update_year INT," +
			"  population_diff INT" +
			") WITH (" +
			"  'connector' = 'filesystem'," +
			"  'path' = '" + populationDirPath + "'," +
			"  'format' = 'csv'" +
			")"
		);

		// insert some example data into the table
		final TableResult insertionResult = env.executeSql(
			"INSERT INTO PopulationUpdates VALUES" +
			"  ('Los Angeles', 'CA', 2013, 13106100), " +
			"  ('Los Angeles', 'CA', 2014, 72600), " +
			"  ('Los Angeles', 'CA', 2015, 72300), " +
			"  ('Chicago', 'IL', 2013, 9553270), " +
			"  ('Chicago', 'IL', 2014, 11340), " +
			"  ('Chicago', 'IL', 2015, -6730), " +
			"  ('Houston', 'TX', 2013, 6330660), " +
			"  ('Houston', 'TX', 2014, 172960), " +
			"  ('Houston', 'TX', 2015, 172940), " +
			"  ('Phoenix', 'AZ', 2013, 4404680), " +
			"  ('Phoenix', 'AZ', 2014, 86740), " +
			"  ('Phoenix', 'AZ', 2015, 89700), " +
			"  ('San Antonio', 'TX', 2013, 2280580), " +
			"  ('San Antonio', 'TX', 2014, 49180), " +
			"  ('San Antonio', 'TX', 2015, 50870), " +
			"  ('San Francisco', 'CA', 2013, 4521310), " +
			"  ('San Francisco', 'CA', 2014, 65940), " +
			"  ('San Francisco', 'CA', 2015, 62290), " +
			"  ('Dallas', 'TX', 2013, 6817520), " +
			"  ('Dallas', 'TX', 2014, 137740), " +
			"  ('Dallas', 'TX', 2015, 154020)"
		);

		// since all cluster operations of the Table API are executed asynchronously,
		// we need to wait until the insertion has been completed,
		// an exception is thrown in case of an error
		insertionResult.await();

		// read from table and aggregate the total population per city
		final Table currentPopulation = env.sqlQuery(
			"SELECT city, state, MAX(update_year) AS latest_year, SUM(population_diff) AS population " +
			"FROM PopulationUpdates " +
			"GROUP BY city, state"
		);

		// either define a nested SQL statement with sub-queries
		// or divide the problem into sub-views which will be optimized
		// as a whole during planning
		env.createTemporaryView("CurrentPopulation", currentPopulation);

		// find the top 2 cities with the highest population per state,
		// we use a sub-query that is correlated with every unique state,
		// for every state we rank by population and return the top 2 cities
		final Table topCitiesPerState = env.sqlQuery(
			"SELECT state, city, latest_year, population " +
			"FROM " +
			"  (SELECT DISTINCT state FROM CurrentPopulation) States," +
			"  LATERAL (" +
			"    SELECT city, latest_year, population" +
			"    FROM CurrentPopulation" +
			"    WHERE state = States.state" +
			"    ORDER BY population DESC, latest_year DESC" +
			"    LIMIT 2" +
			"  )"
		);

		// uncomment the following line to get insights into the continuously evaluated query,
		// execute this pipeline by using the local client as an implicit sink
		// topCitiesPerState.execute().print();

		// because we execute the pipeline in streaming mode, the output shows how the result evolves
		// and is updated over time when new information for a city is ingested:
		// +----+-------+-------------+-------------+-------------+
		// | op | state |        city | latest_year |  population |
		// +----+-------+-------------+-------------+-------------+
		// | +I |    CA | Los Angeles |        2013 |    13106100 |
		// | -D |    CA | Los Angeles |        2013 |    13106100 |
		// | +I |    CA | Los Angeles |        2014 |    13178700 |
		// | -D |    CA | Los Angeles |        2014 |    13178700 |
		// | +I |    CA | Los Angeles |        2015 |    13251000 |
		// ...

		// the changelog can be applied (i.e. materialized) in an external system,
		// usually a key-value store can be used as a table sink,
		// but to show the underlying changelog capabilities we simply use
		// execute().collect() and a List where we maintain updates
		try (CloseableIterator<Row> iterator = topCitiesPerState.execute().collect()) {
			final List<Row> materializedUpdates = new ArrayList<>();
			iterator.forEachRemaining(row -> {
				final RowKind kind = row.getKind();
				switch (kind) {
					case INSERT:
					case UPDATE_BEFORE:
						row.setKind(RowKind.INSERT); // for full equality
						materializedUpdates.add(row);
						break;
					case UPDATE_AFTER:
					case DELETE:
						row.setKind(RowKind.INSERT); // for full equality
						materializedUpdates.remove(row);
						break;
				}
			});
			// show the final output table if the result is bounded,
			// the output should exclude San Antonio because it has a smaller population than
			// Houston or Dallas in Texas (TX)
			materializedUpdates.forEach(System.out::println);
		}
	}

	/**
	 * Creates an empty temporary directory for CSV files and returns the absolute path.
	 */
	private static String createTemporaryDirectory() throws IOException {
		final Path tempDirectory = Files.createTempDirectory("population");
		return tempDirectory.toString();
	}
}
