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

package eu.stratosphere.example.record.shortestpaths;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.functions.CoGroupFunction;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.record.io.DelimitedInputFormat;
import eu.stratosphere.api.record.io.FileOutputFormat;
import eu.stratosphere.api.record.operators.CoGroupOperator;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;
import eu.stratosphere.util.Collector;

/**
 * Implementation of the Pairwise Shortest Path example PACT program.
 * The program implements one iteration of the algorithm and must be run multiple times until no changes are computed.
 * 
 * The pairwise shortest path algorithm comes from the domain graph problems. The goal is to find all shortest paths
 * between any two transitively connected nodes in a graph. In this implementation edges are interpreted as directed and weighted.
 * 
 * For the first iteration, the program allows two input formats:
 * 1) RDF triples with foaf:knows predicates. A triple is interpreted as an edge from the RDF subject to the RDF object with weight 1.
 * 2) The programs text-serialization for paths (see @see PathInFormat and @see PathOutFormat). 
 * 
 * The RDF input format is used if the 4th parameter of the getPlan() method is set to "true". If set to "false" the path input format is used. 
 *  
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * @author Moritz Kaufmann (moritz.kaufmann@campus.tu-berlin.de)
 *
 */
public class PairwiseSP implements Program, ProgramDescription {

	/**
	 * Reads RDF triples and filters on the foaf:knows RDF predicate. The triples elements must be separated by whitespaces.
	 * The foaf:knows RDF predicate indicates that the RDF subject knows the object (typically of type foaf:person).
	 * The connections between people are extracted and handles as graph edges. For the Pairwise Shortest Path algorithm the 
	 * connection is interpreted as a directed edge, i.e. subject knows object, but the object does not necessarily know the subject.
	 * 
	 * The RDFTripleInFormat filters all RDF triples with foaf:knows predicates. 
	 * For each triple with foaf:knows predicate, a record is emitted with 
	 * - from-node being the RDF subject at field position 0,
     * - to-node being the RDF object at field position 1,
     * - length being 1 at field position 2, and 
     * - hopList being an empty string at field position 3. 
	 *
	 */
	public static class RDFTripleInFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;

		private final PactString fromNode = new PactString();
		private final PactString toNode = new PactString();
		private final PactInteger pathLength = new PactInteger(1);
		private final PactInteger hopCnt = new PactInteger(0);
		private final PactString hopList = new PactString(" ");
		
		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
			String lineStr = new String(bytes, offset, numBytes);
			// replace reduce whitespaces and trim
			lineStr = lineStr.replaceAll("\\s+", " ").trim();
			// build whitespace tokenizer
			StringTokenizer st = new StringTokenizer(lineStr, " ");

			// line must have at least three elements
			if (st.countTokens() < 3)
				return false;

			String rdfSubj = st.nextToken();
			String rdfPred = st.nextToken();
			String rdfObj = st.nextToken();

			// we only want foaf:knows predicates
			if (!rdfPred.equals("<http://xmlns.com/foaf/0.1/knows>"))
				return false;

			// build node pair from subject and object
			fromNode.setValue(rdfSubj);
			toNode.setValue(rdfObj);
						
			target.setField(0, fromNode);
			target.setField(1, toNode);
			target.setField(2, pathLength);
			target.setField(3, hopCnt);
			target.setField(4, hopList);

			return true;
		}
	}
	
	/**
	 * The PathInFormat reads paths consisting of a from-node a to-node, a length, and hop node list serialized as a string.
	 * All four elements of the path must be separated by the pipe character ('|') and may not contain any pipe characters itself.
	 * 
	 * PathInFormat returns records with:
	 * - from-node at field position 0,
	 * - to-node at field position 1,
	 * - length at field position 2,
	 * - hop list at field position 3. 
	 */
	public static class PathInFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;
		
		private final PactString fromNode = new PactString();
		private final PactString toNode = new PactString();
		private final PactInteger length = new PactInteger();
		private final PactInteger hopCnt = new PactInteger();
		private final PactString hopList = new PactString();

		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
			String lineStr = new String(bytes, offset, numBytes);
			StringTokenizer st = new StringTokenizer(lineStr, "|");
			
			// path must have exactly 5 tokens (fromNode, toNode, length, hopCnt, hopList)
			if (st.countTokens() != 5) return false;
			
			this.fromNode.setValue(st.nextToken());
			this.toNode.setValue(st.nextToken());
			this.length.setValue(Integer.parseInt(st.nextToken()));
			this.hopCnt.setValue(Integer.parseInt(st.nextToken()));
			this.hopList.setValue(st.nextToken());

			target.setField(0, fromNode);
			target.setField(1, toNode);
			target.setField(2, length);
			target.setField(3, hopCnt);
			target.setField(4, hopList);
			
			return true;
		}
	}

	/**
	 * The PathOutFormat serializes paths to text. 
	 * In order, the from-node, the to-node, the length, the hop list are written out.
	 * Elements are separated by the pipe character ('|').  
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class PathOutFormat extends FileOutputFormat {
		private static final long serialVersionUID = 1L;

		@Override
		public void writeRecord(PactRecord record) throws IOException {
			StringBuilder line = new StringBuilder();

			// append from-node
			line.append(record.getField(0, PactString.class).toString());
			line.append("|");
			// append to-node
			line.append(record.getField(1, PactString.class).toString());
			line.append("|");
			// append length
			line.append(record.getField(2, PactInteger.class).toString());
			line.append("|");
			// append hopCnt
			line.append(record.getField(3, PactInteger.class).toString());
			line.append("|");
			// append hopList
			line.append(record.getField(4, PactString.class).toString());
			line.append("|");
			line.append("\n");
			
			stream.write(line.toString().getBytes());
		}
	}

	/**
	 * Concatenates two paths where the from-node of the first path and the to-node of the second path are the same.
	 * The second input path becomes the first part and the first input path the second part of the output path.
	 * The length of the output path is the sum of both input paths. 
	 * The output path's hops list is built from both path's hops lists and the common node.  
	 */
	@ConstantFieldsFirst(1)
	@ConstantFieldsSecond(0)
	public static class ConcatPaths extends JoinFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final PactRecord outputRecord = new PactRecord();
		
		private final PactInteger length = new PactInteger();
		private final PactInteger hopCnt = new PactInteger();
		private final PactString hopList = new PactString();
		
		@Override
		public void match(PactRecord rec1, PactRecord rec2, Collector<PactRecord> out) throws Exception {

			// rec1 has matching start, rec2 matching end
			// Therefore, rec2's end node and rec1's start node are identical
			// First half of new path will be rec2, second half will be rec1
			
			// Get from-node and to-node of new path  
			final PactString fromNode = rec2.getField(0, PactString.class);
			final PactString toNode = rec1.getField(1, PactString.class);
			
			// Check whether from-node = to-node to prevent circles!
			if (fromNode.equals(toNode)) return;

			// Create new path
			outputRecord.setField(0, fromNode);
			outputRecord.setField(1, toNode);
			
			// Compute length of new path
			length.setValue(rec1.getField(2, PactInteger.class).getValue() + rec2.getField(2, PactInteger.class).getValue());
			outputRecord.setField(2, length);
			
			// compute hop count
			int hops = rec1.getField(3, PactInteger.class).getValue() + 1 + rec2.getField(3, PactInteger.class).getValue();
			hopCnt.setValue(hops);
			outputRecord.setField(3, hopCnt);
			
			// Concatenate hops lists and insert matching node
			StringBuilder sb = new StringBuilder();
			// first path
			sb.append(rec2.getField(4, PactString.class).getValue());
			sb.append(" ");
			// common node
			sb.append(rec1.getField(0, PactString.class).getValue());
			// second path
			sb.append(" ");
			sb.append(rec1.getField(4, PactString.class).getValue());
						
			hopList.setValue(sb.toString().trim());
			outputRecord.setField(4, hopList);
						
			out.collect(outputRecord);
		}
	}

	/**
	 * Gets two lists of paths as input and emits for each included from-node/to-node combination the shortest path(s).
	 * If for a combination more than one shortest path exists, all shortest paths are emitted. 
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	@ConstantFieldsFirst({0,1})
	@ConstantFieldsSecond({0,1})
	public static class FindShortestPath extends CoGroupFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final PactRecord outputRecord = new PactRecord();
		
		private final Set<PactString> shortestPaths = new HashSet<PactString>();
		private final Map<PactString,PactInteger> hopCnts = new HashMap<PactString,PactInteger>();
		private final PactInteger minLength = new PactInteger();
		
		@Override
		public void coGroup(Iterator<PactRecord> inputRecords, Iterator<PactRecord> concatRecords, Collector<PactRecord> out) {

			// init minimum length and minimum path
			PactRecord pathRec = null;
			PactString path = null;
			if(inputRecords.hasNext()) {
				// path is in input paths
				pathRec = inputRecords.next();
			} else {
				// path must be in concat paths
				pathRec = concatRecords.next();
			}
			// get from node (common for all paths)
			PactString fromNode = pathRec.getField(0, PactString.class);
			// get to node (common for all paths)
			PactString toNode = pathRec.getField(1, PactString.class);
			// get length of path
			minLength.setValue(pathRec.getField(2, PactInteger.class).getValue());
			// store path and hop count
			path = new PactString(pathRec.getField(4, PactString.class));
			shortestPaths.add(path);
			hopCnts.put(path, new PactInteger(pathRec.getField(3, PactInteger.class).getValue()));
						
			// find shortest path of all input paths
			while (inputRecords.hasNext()) {
				pathRec = inputRecords.next();
				PactInteger length = pathRec.getField(2, PactInteger.class);
				
				if (length.getValue() == minLength.getValue()) {
					// path has also minimum length add to list
					path = new PactString(pathRec.getField(4, PactString.class));
					if(shortestPaths.add(path)) {
						hopCnts.put(path, new PactInteger(pathRec.getField(3, PactInteger.class).getValue()));
					}
				} else if (length.getValue() < minLength.getValue()) {
					// path has minimum length
					minLength.setValue(length.getValue());
					// clear lists
					hopCnts.clear();
					shortestPaths.clear();
					// get path and add path and hop count
					path = new PactString(pathRec.getField(4, PactString.class));
					shortestPaths.add(path);
					hopCnts.put(path, new PactInteger(pathRec.getField(3, PactInteger.class).getValue()));
				}
			}

			// find shortest path of all input and concatenated paths
			while (concatRecords.hasNext()) {
				pathRec = concatRecords.next();
				PactInteger length = pathRec.getField(2, PactInteger.class);
				
				if (length.getValue() == minLength.getValue()) {
					// path has also minimum length add to list
					path = new PactString(pathRec.getField(4, PactString.class));
					if(shortestPaths.add(path)) {
						hopCnts.put(path, new PactInteger(pathRec.getField(3, PactInteger.class).getValue()));
					}
				} else if (length.getValue() < minLength.getValue()) {
					// path has minimum length
					minLength.setValue(length.getValue());
					// clear lists
					hopCnts.clear();
					shortestPaths.clear();
					// get path and add path and hop count
					path = new PactString(pathRec.getField(4, PactString.class));
					shortestPaths.add(path);
					hopCnts.put(path, new PactInteger(pathRec.getField(3, PactInteger.class).getValue()));
				}
			}
			
			outputRecord.setField(0, fromNode);
			outputRecord.setField(1, toNode);
			outputRecord.setField(2, minLength);
			
			// emit all shortest paths
			for(PactString shortestPath : shortestPaths) {
				outputRecord.setField(3, hopCnts.get(shortestPath));
				outputRecord.setField(4, shortestPath);
				out.collect(outputRecord);
			}
									
			hopCnts.clear();
			shortestPaths.clear();
			
		}
	}

	/**
	 * Assembles the Plan of the Pairwise Shortest Paths example Pact program.
	 * The program computes one iteration of the Pairwise Shortest Paths algorithm.
	 * 
	 * For the first iteration, two input formats can be chosen:
	 * 1) RDF triples with foaf:knows predicates
	 * 2) Text-serialized paths (see PathInFormat and PathOutFormat)
	 * 
	 * To choose 1) set the forth parameter to "true". If set to "false" 2) will be used.
	 *
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String paths     = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");
		boolean rdfInput = (args.length > 3 ? Boolean.parseBoolean(args[3]) : false);

		FileDataSource pathsInput;
		
		if(rdfInput) {
			pathsInput = new FileDataSource(new RDFTripleInFormat(), paths, "RDF Triples");
		} else {
			pathsInput = new FileDataSource(new PathInFormat(), paths, "Paths");
		}
		pathsInput.setDegreeOfParallelism(numSubTasks);

		JoinOperator concatPaths = 
				JoinOperator.builder(new ConcatPaths(), PactString.class, 0, 1)
			.name("Concat Paths")
			.build();

		concatPaths.setDegreeOfParallelism(numSubTasks);

		CoGroupOperator findShortestPaths = 
				CoGroupOperator.builder(new FindShortestPath(), PactString.class, 0, 0)
			.keyField(PactString.class, 1, 1)
			.name("Find Shortest Paths")
			.build();
		findShortestPaths.setDegreeOfParallelism(numSubTasks);

		FileDataSink result = new FileDataSink(new PathOutFormat(),output, "New Paths");
		result.setDegreeOfParallelism(numSubTasks);

		result.addInput(findShortestPaths);
		findShortestPaths.addFirstInput(pathsInput);
		findShortestPaths.addSecondInput(concatPaths);
		concatPaths.addFirstInput(pathsInput);
		concatPaths.addSecondInput(pathsInput);

		return new Plan(result, "Pairwise Shortest Paths");

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks], [inputPaths], [outputPaths], [RDFInputFlag]";
	}
}
