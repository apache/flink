package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;

public class TeraSort implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {

		return "Parameters: [noSubStasks] [input] [output]";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {

		// parse job parameters
		final int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String input = (args.length > 1 ? args[1] : "");
		final String output = (args.length > 2 ? args[2] : "");
		
		System.out.println("Number of subtasks: " + noSubTasks);
		System.out.println("Input path: " + input);
		System.out.println("Output path: " + output);

		final DataSourceContract<TeraKey, TeraValue> source = new DataSourceContract<TeraKey, TeraValue>(
				TeraInputFormat.class, input, "Data Source");
		source.setDegreeOfParallelism(noSubTasks);

		final DataSinkContract<TeraKey, TeraValue> sink = new DataSinkContract<TeraKey, TeraValue>(
			TeraOutputFormat.class, output, "Data Sink");
		sink.setDegreeOfParallelism(noSubTasks);

		sink.setInput(source);

		return new Plan(sink, "TeraSort");
	}

}
