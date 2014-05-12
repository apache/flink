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

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.recordJobs.graph.PairwiseSP;
import eu.stratosphere.test.util.RecordAPITestBase;

@RunWith(Parameterized.class)
public class PairwiseSPITCase extends RecordAPITestBase {

	String rdfDataPath = null;
	String resultPath = null;

	/*
	private String paths = "A|C|7| |\n" + "A|D|6| |\n" + "B|A|1| |\n" + "B|D|2| |\n" + "C|B|3| |\n" + "C|E|10| |\n"
			+ "C|F|12| |\n" + "C|G|9| |\n" + "D|F|5| |\n" + "E|H|2| |\n" + "F|E|3| |\n" + "G|F|1| |\n" + "H|D|2| |\n"
			+ "H|E|4| |\n";
	*/
	
	private static final String RDF_DATA = "<A> <http://xmlns.com/foaf/0.1/knows> <C>\n" + "<A> <http://xmlns.com/foaf/0.1/knows> <D>\n" +
	                         "<B> <http://xmlns.com/foaf/0.1/knows> <A>\n" + "<B> <http://xmlns.com/foaf/0.1/knows> <D>\n" +
	                         "<C> <http://xmlns.com/foaf/0.1/knows> <B>\n" + "<C> <http://xmlns.com/foaf/0.1/knows> <E>\n" +
	                         "<C> <http://xmlns.com/foaf/0.1/knows> <F>\n" + "<C> <http://xmlns.com/foaf/0.1/knows> <G>\n" +
	                         "<D> <http://xmlns.com/foaf/0.1/knows> <F>\n" + "<E> <http://xmlns.com/foaf/0.1/knows> <H>\n" +
	                         "<F> <http://xmlns.com/foaf/0.1/knows> <E>\n" + "<G> <http://xmlns.com/foaf/0.1/knows> <F>\n" +
	                         "<H> <http://xmlns.com/foaf/0.1/knows> <D>\n" + "<H> <http://xmlns.com/foaf/0.1/knows> <E>\n";

	private static final String EXPECTED = "<A>|<C>|1|0| |\n"     + "<A>|<D>|1|0| |\n"     + "<B>|<A>|1|0| |\n"     + "<B>|<D>|1|0| |\n"   +
	 						  "<C>|<B>|1|0| |\n"     + "<C>|<E>|1|0| |\n"     + "<C>|<F>|1|0| |\n"     + "<C>|<G>|1|0| |\n"   +
	 						  "<D>|<F>|1|0| |\n"     + "<E>|<H>|1|0| |\n"     + "<F>|<E>|1|0| |\n"     + "<G>|<F>|1|0| |\n"   +
	 						  "<H>|<D>|1|0| |\n"     + "<H>|<E>|1|0| |\n"     + "<A>|<B>|2|1|<C>|\n"   + "<A>|<E>|2|1|<C>|\n" +
	 						  "<A>|<F>|2|1|<C>|\n"   + "<A>|<G>|2|1|<C>|\n"   + "<A>|<F>|2|1|<D>|\n"   + "<B>|<C>|2|1|<A>|\n" + 
	 						  "<B>|<F>|2|1|<D>|\n"   + "<C>|<A>|2|1|<B>|\n"   + "<C>|<D>|2|1|<B>|\n"   + "<C>|<H>|2|1|<E>|\n" +
				              "<D>|<E>|2|1|<F>|\n"   + "<E>|<D>|2|1|<H>|\n"   + "<F>|<H>|2|1|<E>|\n"   + "<G>|<E>|2|1|<F>|\n" + 
				              "<H>|<F>|2|1|<D>|\n";

	public PairwiseSPITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		rdfDataPath = createTempFile("rdf_data.txt", RDF_DATA);
		resultPath = getTempDirPath("ITER_1");
	}

	@Override
	protected Plan getTestJob() {
		PairwiseSP a2aSP = new PairwiseSP();
		return a2aSP.getPlan(config.getString("All2AllSPTest#NoSubtasks", "4"),
				rdfDataPath,
				resultPath,
				"true");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("All2AllSPTest#NoSubtasks", 4);
		return toParameterList(config);
	}
}
