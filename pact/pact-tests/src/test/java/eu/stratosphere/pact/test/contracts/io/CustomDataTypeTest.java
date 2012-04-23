/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.test.contracts.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.GenericInputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.TestBase;


/**
 * @author Stephan Ewen
 */
@RunWith(Parameterized.class)
public class CustomDataTypeTest extends TestBase 
{
	private static final String CLASS_TO_INSTANTIATE_KEY = "pact.test.instantiation_clazz";
	private static final String CLASS_TO_INSTANTIATE_NAME = "eu.stratosphere.pact.test.external.TestClass";
	private static final String EXTERNAL_JAR_RESOURCE = "eu/stratosphere/pact/test/contracts/io/TestJar.jar";
	
	private final String jarTempPath;
	
	public CustomDataTypeTest(Configuration testConfig)
	{
		super(testConfig);
		
		this.jarTempPath = System.getProperty("java.io.tmpdir") + File.separator + "_" + Integer.toHexString((int) (Math.random() * 1000000000)) + ".jar";
	}
	
	@Override
	protected void preSubmit()
	{
		deleteFileIfPresent(this.jarTempPath);
	}
	
	@Override
	protected void postSubmit() throws Exception
	{
		deleteFileIfPresent(this.jarTempPath);
	}
	
	@Override
	protected JobGraph getJobGraph() throws Exception
	{
		GenericDataSource<EmptyInputFormat> datasource = 
				new GenericDataSource<EmptyInputFormat>(EmptyInputFormat.class, "Source");
		datasource.getParameters().setString(CLASS_TO_INSTANTIATE_KEY, CLASS_TO_INSTANTIATE_NAME);
		
		GenericDataSink sink = new GenericDataSink(BlackholeOutputFormat.class, datasource, "Sink");
		sink.getParameters().setString(CLASS_TO_INSTANTIATE_KEY, CLASS_TO_INSTANTIATE_NAME);
		
		Plan plan = new Plan(sink);
		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);
		
		// copy the jar with the extra class from the resources to the temp folder
		{
			final InputStream is = getClass().getClassLoader().getResourceAsStream(EXTERNAL_JAR_RESOURCE);
			final FileOutputStream fos = new FileOutputStream(this.jarTempPath);
			final byte[] buffer = new byte[1024];
			
			int numRead;
			while ((numRead = is.read(buffer)) != -1) {
				fos.write(buffer, 0, numRead);
			}
			
			is.close();
			fos.close();
		}
				
		// attach the jar for a class that is not on the job-manager's classpath
		jobGraph.addJar(new Path("file://" + this.jarTempPath));
		return jobGraph;
	}
	
	private static final void deleteFileIfPresent(String path)
	{
		final File f = new File(path);
		if (f.exists()) {
			f.delete();
		}
	}
	
	public static final class EmptyInputFormat extends GenericInputFormat
	{
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
		public boolean nextRecord(PactRecord record) throws IOException {
			return false;
		}
	}
	
	public static final class BlackholeOutputFormat extends OutputFormat
	{
		@Override
		public void configure(Configuration parameters) {
			// instantiate some user defined class
			parameters.getClass(CLASS_TO_INSTANTIATE_KEY, Object.class, Object.class);
		}

		@Override
		public void open(int taskNumber) throws IOException {}

		@Override
		public void writeRecord(PactRecord record) throws IOException {}

		@Override
		public void close() throws IOException {}
		
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations()
	{
		List<Object[]> tConfigs = new ArrayList<Object[]>(1);
		Configuration config = new Configuration();
		config.setInteger("EnumTrianglesTest#NoSubtasks", 4);
		tConfigs.add(new Object[] {config});
		return tConfigs;
	}
}
