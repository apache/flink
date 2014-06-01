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

package eu.stratosphere.test.recordJobTests;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.recordJobs.graph.EnumTrianglesRdfFoaf;
import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

@RunWith(Parameterized.class)
public class EnumTrianglesRDFITCase extends RecordAPITestBase {

	String edgesPath = null;
	String resultPath = null; 

	private static final String EDGES = "<a> <http://xmlns.com/foaf/0.1/knows> <b>\n" + "<a> <http://xmlns.com/foaf/0.1/knows> <c>\n" +
						   "<a> <http://xmlns.com/foaf/0.1/knows> <d>\n" + "<b> <http://xmlns.com/foaf/0.1/knows> <c>\n" + 
						   "<b> <http://xmlns.com/foaf/0.1/knows> <e>\n" + "<b> <http://xmlns.com/foaf/0.1/knows> <f>\n" + 
						   "<c> <http://xmlns.com/foaf/0.1/knows> <d>\n" + "<d> <http://xmlns.com/foaf/0.1/knows> <b>\n" + 
						   "<f> <http://xmlns.com/foaf/0.1/knows> <g>\n" + "<f> <http://xmlns.com/foaf/0.1/knows> <h>\n" + 
						   "<f> <http://xmlns.com/foaf/0.1/knows> <i>\n" + "<g> <http://xmlns.com/foaf/0.1/knows> <i>\n" +
						   "<g> <http://willNotWork> <h>\n";

	private static final String EXPECTED = "<a> <b> <c>\n" + "<a> <b> <d>\n" + "<a> <c> <d>\n" +
	                          "<b> <c> <d>\n" + "<f> <g> <i>\n";
	
	public EnumTrianglesRDFITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		edgesPath = createTempFile("edges.txt", EDGES);
		resultPath = getTempDirPath("triangles");
	}

	@Override
	protected Plan getTestJob() {
		EnumTrianglesRdfFoaf enumTriangles = new EnumTrianglesRdfFoaf();
		return enumTriangles.getPlan(
				config.getString("EnumTrianglesTest#NoSubtasks", new Integer(DOP).toString()), edgesPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("EnumTrianglesTest#NoSubtasks", DOP);
		return toParameterList(config);
	}
}
