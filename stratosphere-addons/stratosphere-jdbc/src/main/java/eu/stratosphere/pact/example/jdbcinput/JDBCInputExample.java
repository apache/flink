package eu.stratosphere.pact.example.jdbcinput;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.JDBCInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactFloat;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * DB Schema
 * ID | title | author | price | qty / int | varchar | varchar | float | int
 */
public class JDBCInputExample implements PlanAssembler, PlanAssemblerDescription {

	public static void execute(Plan toExecute) throws Exception {
		LocalExecutor executor = new LocalExecutor();
		executor.start();
		long runtime = executor.executePlan(toExecute);
		System.out.println("runtime:  " + runtime);
		executor.stop();
	}

	@Override
	public Plan getPlan(String ... args) {
		// String url = args [0];
		String url = "jdbc:mysql://127.0.0.1:3306/ebookshop?user=root&password=1111";
		// String query = args[1];
		String query = "select * from books;";
		// String output = args[2];
		String output = "file://c:/TEST/output.txt";

		/*
		 * In this example we use the constructor where the url contains all the settings that are needed.
		 * You could also use the default constructor and deliver a Configuration with all the needed settings.
		 * You also could set the settings to the source-instance.
		 */
		GenericDataSource<JDBCInputFormat> source = new GenericDataSource<JDBCInputFormat>(
				new JDBCInputFormat("com.mysql.jdbc.Driver", url, query), "Data Source");

		FileDataSink sink = new FileDataSink(new RecordOutputFormat(), output, "Data Output");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0)
			.field(PactString.class, 1)
			.field(PactString.class, 2)
			.field(PactFloat.class, 3)
			.field(PactInteger.class, 4);

		sink.addInput(source);
		return new Plan(sink, "JDBC Input Example Job");
	}

	@Override
	public String getDescription() {
		return "Parameter: [URL] [Query] [Output File]"; // TODO
	}

	// You can run this using:
	// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.quickstart.RunJob <args>"
	public static void main(String[] args) throws Exception {
		JDBCInputExample tut = new JDBCInputExample();
		Plan toExecute = tut.getPlan(args);
		execute(toExecute);
	}
}
