/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.operators.io;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.io.OutputFormat;
import eu.stratosphere.api.operators.GenericDataSink;
import eu.stratosphere.api.operators.GenericDataSource;
import eu.stratosphere.api.record.io.GenericInputFormat;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.util.TestBase;
import eu.stratosphere.types.Record;


@RunWith(Parameterized.class)
public class CustomDataTypeTest extends TestBase 
{
	private static final String CLASS_TO_INSTANTIATE_KEY = "pact.test.instantiation_clazz";
	private static final String CLASS_TO_INSTANTIATE_NAME = "eu.stratosphere.pact.test.external.TestClass";
	private static final String EXTERNAL_JAR_RESOURCE = "/CustomDataTypeTest/CustomDataTypeTest.jar";
	
	public CustomDataTypeTest(Configuration testConfig)
	{
		super(testConfig);
	}
	
	@Override
	protected void preSubmit()
	{
	}
	
	@Override
	protected void postSubmit() throws Exception {}
	
	@Override
	protected JobGraph getJobGraph() throws Exception {
		GenericDataSource<EmptyInputFormat> datasource = 
				new GenericDataSource<EmptyInputFormat>(new EmptyInputFormat(), "Source");
		datasource.getParameters().setString(CLASS_TO_INSTANTIATE_KEY, CLASS_TO_INSTANTIATE_NAME);
		
		GenericDataSink sink = new GenericDataSink(new BlackholeOutputFormat(), datasource, "Sink");
		sink.getParameters().setString(CLASS_TO_INSTANTIATE_KEY, CLASS_TO_INSTANTIATE_NAME);
		
		Plan plan = new Plan(sink);
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);
		
		URL jarFileURL = getClass().getResource(EXTERNAL_JAR_RESOURCE);
		
		// attach the jar for a class that is not on the job-manager's classpath
		jobGraph.addJar(new Path(jarFileURL.toURI().toString()));
	
		return jobGraph;
	}
	
	public static final class EmptyInputFormat extends GenericInputFormat {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void configure(Configuration parameters)	{
			super.configure(parameters);
			// instantiate some user defined class
			parameters.getClass(CLASS_TO_INSTANTIATE_KEY, Object.class, Object.class);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return true;
		}

		@Override
		public boolean nextRecord(Record record) throws IOException {
			return false;
		}
	}
	
	public static final class BlackholeOutputFormat implements OutputFormat<Record> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void configure(Configuration parameters) {
			// instantiate some user defined class
			parameters.getClass(CLASS_TO_INSTANTIATE_KEY, Object.class, Object.class);
		}

		@Override
		public void open(int taskNumber) throws IOException {}

		@Override
		public void writeRecord(Record record) throws IOException {}

		@Override
		public void close() throws IOException {}
		
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() {
		List<Object[]> tConfigs = new ArrayList<Object[]>(1);
		Configuration config = new Configuration();
		config.setInteger("EnumTrianglesTest#NoSubtasks", 4);
		tConfigs.add(new Object[] {config});
		return tConfigs;
	}
}
