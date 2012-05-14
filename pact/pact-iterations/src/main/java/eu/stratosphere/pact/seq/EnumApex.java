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

package eu.stratosphere.pact.seq;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.iterative.nephele.util.NepheleUtil;

/**
 * Implementation of the triangle enumeration example Pact program.
 * The program expects a file with RDF triples (in XML serialization) as input. Triples must be separated by linebrakes.
 * 
 * The program filters for foaf:knows predicates to identify relationships between two entities (typically persons).
 * Relationships are interpreted as edges in a social graph. Then the program enumerates all triangles which are build 
 * by edges in that graph. 
 * 
 * Usually, triangle enumeration is used as a pre-processing step to identify highly connected subgraphs.
 * The algorithm was published as MapReduce job by J. Cohen in "Graph Twiddling in a MapReduce World".
 * The Pact version was described in "MapReduce and PACT - Comparing Data Parallel Programming Models" (BTW 2011). 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
 *
 */
public class EnumApex implements PlanAssembler, PlanAssemblerDescription {
	
	/**
	 * Reads RDF triples and filters on the foaf:knows RDF predicate.
	 * The foaf:knows RDF predicate indicates that the RDF subject and object (typically of type foaf:person) know each
	 * other.
	 * Therefore, knowing connections between people are extracted and handles as graph edges.
	 * The EdgeListInFormat filters all rdf triples with foaf:knows predicates. The subjects and objects URL are
	 * compared.
	 * The lexicographically smaller URL becomes the first part (node) of the edge, the greater one the second part
	 * (node).
	 * Finally, the format emits the edge as key. The value is not required, so its set to NULL.
	 * 
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class EdgeListInFormat extends DelimitedInputFormat {

		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {

			String lineStr = new String(bytes, 0, numBytes);
			int splitPoint = lineStr.indexOf(',');
			
			//Return if no delimiter is found
			if(splitPoint == -1) {
				return false;
			}
			
			long idA = Long.parseLong(lineStr.substring(0, splitPoint));
			long idB = Long.parseLong(lineStr.substring(splitPoint+1));

			target.setField(0, new PactLong(idA));
			target.setField(1, new PactLong(idB));
			
			return true;
		}
	}

	/**
	 * Used to write an EdgeList to text file.
	 * All edges of the list are concatenated and serialized to byte string.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class EdgeApexOutFormat extends FileOutputFormat {
		private static final Log LOG = LogFactory.getLog(EdgeApexOutFormat.class);

		@Override
		public void writeRecord(PactRecord record) throws IOException {
			StringBuilder line = new StringBuilder();
			PactLong longTarget = new PactLong();
			
			line.append(record.getField(0, longTarget).getValue());
			line.append(' ');
			line.append(record.getField(1, longTarget).getValue());
			
			LongList apexes = record.getField(2, LongList.class);
			long[] ids = apexes.getNumbers();
			for (long id : ids) {
				line.append(' ');
				line.append(id);
			}
			
			line.append("\n");
			
			stream.write(line.toString().getBytes());
			LOG.debug("Writing out: " + line);
		}
	}

	/**
	 * Transforms key-value pairs of the form (Edge,Null) to (PactString,Edge) where the key becomes the
	 * first node of the input key and the value becomes the input key.
	 * Due to the input format, the first node of the input edge is the lexicographically smaller of both nodes.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class AssignKeys extends MapStub {

		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			PactLong longTarget = new PactLong();
			
			long idA = record.getField(0, longTarget).getValue();
			long idB = record.getField(1, longTarget).getValue();
			
			//Fix order if the vertexes are wrong
			if(idA > idB) {
				longTarget.setValue(idA);
				PactLong longB = new PactLong(idB);
				record.setField(0, longB);
				record.setField(1, longTarget);
			}
			
			out.collect(record);
		}
	}

	/**
	 * Builds triads (open triangles) from two edges that share the same key.
	 * A triad is represented as an EdgeList with two elements.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class BuildTriads extends MatchStub {

		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out)
				throws Exception {
			PactLong longTargetB1 = new PactLong();
			PactLong longTargetB2 = new PactLong();
			longTargetB1 = value1.getField(1, PactLong.class);
			longTargetB2 = value2.getField(1, PactLong.class);
			
			long idB1 = longTargetB1.getValue();
			long idB2 = longTargetB1.getValue();
			
			// we do not connect a node with itself
			if(idB1 == idB2) {
				return;
			}
			
			PactRecord outRecord = value1;
			//Set missing edge
			if(idB1 < idB2) {
				value1.setField(1, longTargetB1);
				value1.setField(2, longTargetB2);
			} else {
				value1.setField(1, longTargetB2);
				value1.setField(2, longTargetB1);
			}
			
			// emit missing edge and triad
			out.collect(outRecord);
		}		
	}

	/**
	 * Matches all missing edges with existing edges from input.
	 * If the missing edge for a triad is found, the triad is transformed to a triangle by adding the missing edge.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class CloseTriadsBuildApex extends MatchStub {
		
		@Override
		public void match(PactRecord triad, PactRecord missingEdge, Collector out) throws Exception {
			PactLong longTarget = new PactLong();
			PactLong[] vertices = new PactLong[3];
			for (int i = 0; i < 3; i++) {
				vertices[i] = new PactLong(triad.getField(i, longTarget).getValue());
			}
			
			//Output three apex variants
			PactRecord apex = triad;
			
			//First variant (0-1 edge, 2 apex)
			out.collect(apex);
			
			//Second variant(1-2 edge, 0 apex)
			apex.setField(0, vertices[1]);
			apex.setField(1, vertices[2]);
			apex.setField(2, vertices[0]);
			out.collect(apex);
			
			//Third variant (0-2 edge, 1 apex)
			apex.setField(0, vertices[0]);
			apex.setField(1, vertices[2]);
			apex.setField(2, vertices[1]);
			out.collect(apex);
		} 
	}
	
	/**
	 * Groups by edge to get all triads + apexes the edge is involved in
	 * 
	 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
	 */
	public static class TriadNeighbours extends ReduceStub {

		private LongList apexes = new LongList();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
				throws Exception {
			PactLong longTarget = new PactLong();
			apexes.clear();
			
			//Read all apexes
			PactRecord edgeApex = null;
			while(records.hasNext()) {
				edgeApex = records.next();
				apexes.add(edgeApex.getField(2, longTarget).getValue());
			}
			
			//Output edge with apex list
			edgeApex.setField(2, apexes);
			out.collect(edgeApex);
		}
	}

	/**
	 * Assembles the Plan of the triangle enumeration example Pact program.
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String edgeInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource edges = new FileDataSource(EdgeListInFormat.class, edgeInput);
		edges.setDegreeOfParallelism(noSubTasks);
		//edges.setOutputContract(UniqueKey.class);

		MapContract assignKeys = new MapContract(AssignKeys.class, "Assign Keys");
		assignKeys.setDegreeOfParallelism(noSubTasks);

		MatchContract buildTriads = new MatchContract(BuildTriads.class, PactLong.class, 0, 0, "Build Triads");
		buildTriads.setDegreeOfParallelism(noSubTasks);

		@SuppressWarnings("unchecked")
		MatchContract closeTriads = new MatchContract(CloseTriadsBuildApex.class, new Class[] {PactLong.class, PactLong.class}, 
				new int[] {1, 2}, new int[] {1, 2}, "Building Triangles and Apexes");
		closeTriads.setDegreeOfParallelism(noSubTasks);
		
		@SuppressWarnings("unchecked")
		ReduceContract neighbour = new ReduceContract(TriadNeighbours.class, new Class[] {PactLong.class, PactLong.class}, 
				new int[] {0, 1}, "Build triad neighbours");
		assignKeys.setDegreeOfParallelism(noSubTasks);

		FileDataSink triangles = new FileDataSink(EdgeApexOutFormat.class, output);
		triangles.setDegreeOfParallelism(noSubTasks);

		triangles.setInput(neighbour);
		neighbour.setInput(closeTriads);
		closeTriads.setSecondInput(edges);
		closeTriads.setFirstInput(buildTriads);
		buildTriads.setFirstInput(assignKeys);
		buildTriads.setSecondInput(assignKeys);
		assignKeys.setInput(edges);

		return new Plan(triangles, "Enumerate Triangles");

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [inputRDFTriples] [outputTriangles]";
	}
	
	public static void main(String[] args) throws ProgramInvocationException, CompilerException, ErrorInPlanAssemblerException {
		PactProgram program = new PactProgram(new File("/home/mkaufmann/iter.jar"), "eu.stratosphere.pact.seq.EnumApex", args);
		Client client = new Client(NepheleUtil.getConfiguration());
		client.run(program, true);
	}

}
