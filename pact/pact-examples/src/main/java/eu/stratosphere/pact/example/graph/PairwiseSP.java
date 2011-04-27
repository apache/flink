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

package eu.stratosphere.pact.example.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Implementation of the Pairwise Shortest Path example PACT program.
 * The program implements on iteration of the algorithm and must be run multiple times until no changes are computed.
 * 
 * The pairwise shortest path algorithm comes from the domain graph problems. The goal is to find all shortest paths
 * between any two transitively connected nodes in a graph. In this implementation edges are interpreted as directed and weighted.
 * 
 * For the first iteration, the program allows two input formats.
 * 1) RDF triples with foaf:knows predicates. A triple are interpreted as an edge from the RDF subject to the RDF object with weight 1.
 * 2) The programs text-serialization for paths (see @see PathInFormat and @see PathOutFormat). 
 * 
 * The RDF input format is used if the 4th parameter of the getPlan() method is set to "true". If set to "false" the path input format is used. 
 *  
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class PairwiseSP implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Simple extension of PactPair to hold two nodes represented as strings.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class NodePair extends PactPair<PactString, PactString> {
		
		public NodePair() {
			super();
		}

		public NodePair(PactString s1, PactString s2) {
			super(s1, s2);
		}

		public String toString() {
			return getFirst().toString() + " " + getSecond();
		}
		
	}

	/**
	 * Holds information about a path from one node (from-node) to another node (to-node).
	 * The length of the path and all nodes on the path (hops) are stored.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class Path implements Value {

		// path from from-node to to-node
		String fromNode;
		String toNode;

		// length of the path
		int length;

		// all nodes on the path (hops)
		String[] hopsList;
		int hopCnt;

		/**
		 * Creates an empty path
		 */
		public Path() {
			hopsList = new String[8];
			hopCnt = 0;
		}
		
		/**
		 * Creates a path from from-node to to-node with an initial length.
		 * The path is a direct connection of both nodes since no hops are specified.
		 * 
		 * @param fromNode The starting node of the path.
		 * @param toNode The ending node of the path.
		 * @param length The length of the path.
		 */
		public Path(String fromNode, String toNode, int length) {
			this.fromNode = fromNode;
			this.toNode = toNode;
			this.length = length;
			hopsList = new String[8];
			hopCnt = 0;
		}

		/**
		 * Creates a path from from-node to to-node with a given length and hops list.
		 * The hops list should be ordered to fully specify the path.
		 * 
		 * @param fromNode The starting node of the path.
		 * @param toNode The ending node of the path.
		 * @param length The length of the path.
		 * @param hopsList The intermediate nodes (hops) of the path.
		 * @param hopCnt The number of hops of the path.
		 */
		public Path(String fromNode, String toNode, int length, String[] hopsList, int hopCnt) {
			this.fromNode = fromNode;
			this.toNode = toNode;
			this.length = length;
			this.hopsList = hopsList;
			this.hopCnt = hopCnt;
		}

		/**
		 * Returns the starting node of the path.
		 * 
		 * @return The starting node of the path.
		 */
		public String getFromNode() {
			return fromNode;
		}

		/**
		 * Returns the ending node of the path.
		 * 
		 * @return The ending node of the path.
		 */
		public String getToNode() {
			return toNode;
		}

		/**
		 * Returns the length of the path.
		 * 
		 * @return The length of the path.
		 */
		public int getLength() {
			return length;
		}

		/**
		 * Returns an array with all intermediate nodes of the path.
		 * 
		 * @return An array with all intermediate nodes of the path.
		 */
		public String[] getHopsList() {
			return hopsList;
		}

		/**
		 * Returns the number of intermediate nodes of the path. 
		 * 
		 * @return The number of intermediate nodes of the path.
		 */
		public int getHopCnt() {
			return hopCnt;
		}

		/**
		 * Sets the starting node of the path.
		 * 
		 * @param fromNode The new starting node of the path.
		 */
		public void setFromNode(String fromNode) {
			this.fromNode = fromNode;
		}

		/**
		 * Sets the ending node of the path.
		 * 
		 * @param toNode The new ending node of the path.
		 */
		public void setToNode(String toNode) {
			this.toNode = toNode;
		}

		/**
		 * Updates the length of the path.
		 * 
		 * @param length The new length of the path.
		 */
		public void setLength(int length) {
			this.length = length;
		}

		/**
		 * Adds an intermediate node at the end of the hops list.
		 * 
		 * @param hop The node which is appended to the end of the hops list. 
		 */
		public void addHop(String hop) {
			// check if hop list array must be extended
			if (hopCnt == hopsList.length) {
				// create a new array with double size of current one
				String[] newHopList = new String[2 * hopCnt];
				// copy all hops to new list
				for (int i = 0; i < hopCnt; i++) {
					newHopList[i] = hopsList[i];
				}
				// set new list
				hopsList = newHopList;
			}
			// add hop at the end of hop list
			this.hopsList[hopCnt++] = hop;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
		 */
		@Override
		public void read(DataInput in) throws IOException {
			fromNode = in.readUTF();
			toNode = in.readUTF();
			length = in.readInt();
			hopCnt = in.readInt();
			if (hopCnt < 8) {
				hopsList = new String[8];
			} else {
				hopsList = new String[hopCnt * 2];
			}
			for (int i = 0; i < hopCnt; i++) {
				hopsList[i] = in.readUTF();
			}
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(fromNode);
			out.writeUTF(toNode);
			out.writeInt(length);
			out.writeInt(hopCnt);
			for (int i = 0; i < hopCnt; i++) {
				out.writeUTF(hopsList[i]);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuilder returnString = new StringBuilder(fromNode + "|" + toNode + "|" + length + "|");
			for (int i = 0; i < hopCnt; i++) {
				returnString.append(hopsList[i]);
					returnString.append('|');
			}

			return returnString.toString();
		}
	}

	/**
	 * Reads RDF triples and filters on the foaf:knows RDF predicate. The triples elements must be separated by whitespaces.
	 * The foaf:knows RDF predicate indicates that the RDF subject knows the object (typically of type foaf:person).
	 * The connections between people are extracted and handles as graph edges. For the Pairwise Shortest Path algorithm the 
	 * connection is interpreted as a directed edge, i.e. subject knows object, but the object does not necessarily know the subject.
	 * 
	 * The RDFTripleInFormat filters all RDF triples with foaf:knows predicates. 
	 * For each triple with foaf:knows predicate, a path is created where the from-node is set to the RDF subject, 
	 * the to-node to the RDFobject, and length to 1. The path is used as output value.
	 * The key is set to a NodePair, where the first node is the RDF subject and the second node is the RDF object. 
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class RDFTripleInFormat extends TextInputFormat<NodePair, Path> {

		private static final Log LOG = LogFactory.getLog(RDFTripleInFormat.class);
		
		@Override
		public boolean readLine(KeyValuePair<NodePair, Path> pair, byte[] line) {
			
			String lineStr = new String(line);
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
			NodePair edge = new NodePair(new PactString(rdfSubj), new PactString(rdfObj)); 
			// create initial path from subject node to object node with length 1 and no hops
			Path initPath = new Path(rdfSubj, rdfObj, 1);
			
			pair.setKey(edge);
			pair.setValue(initPath);

			LOG.debug("Read in: " + pair.getKey() + " :: " + pair.getValue());
			
			return true;
			
		}
		
	}
	
	
	/**
	 * The PathInFormat reads paths consisting of a from-node a to-node, a length, the number of hop nodes, and a lists of hop nodes.
	 * All elements of the path must be separated by the pipe character ('|').
	 * 
	 * PathInFormat returns key-value-pairs with a NodePair as key and the path as value.
	 * The first element of the NodePair is the from-node of the path and the second element is the to-node of the path.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class PathInFormat extends TextInputFormat<NodePair, Path> {

		private static final Log LOG = LogFactory.getLog(PathInFormat.class);

		@Override
		public boolean readLine(KeyValuePair<NodePair, Path> pair, byte[] line) {

			String lineStr = new String(line);
			StringTokenizer st = new StringTokenizer(lineStr, "|");
			
			// path must have at least 4 tokens (fromNode, toNode, length, hopCnt)
			if (st.countTokens() < 4) return false;
			
			String fromNode = st.nextToken();
			String toNode = st.nextToken();
			int length = Integer.parseInt(st.nextToken());
			int hopCnt = Integer.parseInt(st.nextToken());
			
			// remaining tokens must be hops
			if (st.countTokens() != hopCnt) return false;
			
			// create hop list array. Use larger array to avoid reallocation.
			String[] hops = new String[hopCnt*2];
			for(int i=0;i<hopCnt;i++) {
				hops[i] = st.nextToken();
			}
			
			NodePair nodePair = new NodePair(new PactString(fromNode), new PactString(toNode));
			Path path = new Path(fromNode,toNode,length,hops,hopCnt);

			pair.setKey(nodePair);
			pair.setValue(path);

			LOG.debug("Read in: " + pair.getKey() + " :: " + pair.getValue());
			
			return true;
		}
	}

	/**
	 * The PathOutFormat serializes paths to text. 
	 * In order, the from-node, the to-node, the length, the number of hops, and all hops are written out.
	 * Each element (including each hop) is followed by the pipe character ('|') for separation.  
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class PathOutFormat extends TextOutputFormat<PactNull, Path> {

		private static final Log LOG = LogFactory.getLog(PathInFormat.class);

		@Override
		public byte[] writeLine(KeyValuePair<PactNull, Path> pair) {
			StringBuilder line = new StringBuilder();
			Path path = pair.getValue();
			
			line.append(path.getFromNode()+"|");
			line.append(path.getToNode()+"|");
			line.append(path.getLength()+"|");
			line.append(path.getHopCnt()+"|");
			for(int i=0;i<path.getHopCnt();i++) {
				line.append(path.getHopsList()[i]+"|");
			}
			line.append("\n");
			
			LOG.debug("Writing out: [" + path + "]");

			return line.toString().getBytes();
		}

	}

	/**
	 * Sets the key of the emitted key-value-pairs to the from-node of the input path.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class ProjectPathStart extends MapStub<NodePair, Path, PactString, Path> {

		private static final Log LOG = LogFactory.getLog(ProjectPathStart.class);

		@Override
		public void map(NodePair key, Path value, Collector<PactString, Path> out) {

			LOG.debug("Emit: [" + key.getFirst() + "," + value + "]");
			
			out.collect(key.getFirst(), value);
		}
	}

	/**
	 * Sets the key of the emitted key-value-pair to the to-node of the input path.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class ProjectPathEnd extends MapStub<NodePair, Path, PactString, Path> {

		private static final Log LOG = LogFactory.getLog(ProjectPathEnd.class);

		@Override
		public void map(NodePair key, Path value, Collector<PactString, Path> out) {
			
			LOG.debug("Emit: [" + key.getSecond() + "," + value + "]");
			
			out.collect(key.getSecond(), value);
		}
	}

	/**
	 * Concatenates two paths where the from-node of the first path and the to-node of the second path are the same.
	 * The second input path becomes the first part and the first input path the second part of the output path.
	 * The length of the output path is the sum of both input paths. 
	 * The output path's hops list is built from both path's hops lists and the common node.  
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class ConcatPaths extends MatchStub<PactString, Path, Path, NodePair, Path> {

		private static final Log LOG = LogFactory.getLog(ConcatPaths.class);

		@Override
		public void match(PactString matchNode, Path path1, Path path2, Collector<NodePair, Path> out) {

			LOG.debug("Process: [" + matchNode + "," + path1 + "] + [" + matchNode + "," + path2 + "]");

			// path1 was projected to path start, path2 was projected to path end.
			// Therefore, path2's end node and path1's start node are identical
			// First half of new path will be path2, second half will be path1
			
			// Get from-node and to-node of new path  
			String fromNode = path2.getFromNode();
			String toNode = path1.getToNode();
			
			// Check whether from-node = to-node to prevent circles!
			if (fromNode.equals(toNode)) return;

			// Create new path
			Path value = new Path();
			value.setFromNode(fromNode);
			value.setToNode(toNode);
			// Compute length of new path
			value.setLength(path1.getLength() + path2.getLength());
			// Concatenate hops lists and insert matching node
			for (int i = 0; i < path2.getHopCnt(); i++) {
				value.addHop(path2.getHopsList()[i]);
			}
			value.addHop(matchNode.getValue());
			for (int i = 0; i < path1.getHopCnt(); i++) {
				value.addHop(path1.getHopsList()[i]);
			}

			LOG.debug("Emit: [" + fromNode + "|" + toNode + " , " + value + "]");
			
			out.collect(new NodePair(new PactString(fromNode), new PactString(toNode)), value);
			
		}
	}

	/**
	 * Gets two lists of paths as input and emits for each included from-node/to-node combination the shortest path(s).
	 * If for a combination more than one shortest path exists, all shortest paths are emitted. 
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 *
	 */
	public static class FindShortestPath extends CoGroupStub<NodePair, Path, Path, PactNull, Path> {

		private static final Log LOG = LogFactory.getLog(FindShortestPath.class);

		@Override
		public void coGroup(NodePair key, Iterator<Path> inputPaths, Iterator<Path> concatPaths, Collector<PactNull, Path> out) {

			// init minimum length and minimum path
			int minLength = Integer.MAX_VALUE;
			List<Path> shortestPaths = new ArrayList<Path>();

			// find shortest path of all input paths
			while (inputPaths.hasNext()) {
				Path value = inputPaths.next();
				LOG.debug("Process: [" + key + "," + value + "]");

				if (value.getLength() == minLength) {
					// path has also minimum length add to list
					shortestPaths.add(value);
				} else if (value.getLength() < minLength) {
					// path has minimum length
					minLength = value.getLength();
					// clear list and add
					shortestPaths.clear();
					shortestPaths.add(value);
				}
			}

			// find shortest path of all input and concatenated paths
			while (concatPaths.hasNext()) {
				Path value = concatPaths.next();
				LOG.debug("Process: [" + key + "," + value + "]");
				
				if (value.getLength() == minLength) {
					// path has also minimum length add to list
					shortestPaths.add(value);
				} else if (value.getLength() < minLength) {
					// path has minimum length
					minLength = value.getLength();
					// clear list and add
					shortestPaths.clear();
					shortestPaths.add(value);
				}
			}
			
			// emit all shortest paths
			for(Path shortestPath : shortestPaths) {
				LOG.debug("Emit: [" + key + "," + shortestPath + "]");
				out.collect(new PactNull(), shortestPath);
			}
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
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String paths     = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");
		boolean rdfInput = (args.length > 3 ? Boolean.parseBoolean(args[3]) : false);

		DataSourceContract<NodePair, Path> pathsInput;
		
		if(rdfInput) {
			pathsInput = new DataSourceContract<NodePair, Path>(RDFTripleInFormat.class, paths);
		} else {
			pathsInput = new DataSourceContract<NodePair, Path>(PathInFormat.class, paths);
		}
		pathsInput.setFormatParameter("delimiter", "\n");
		pathsInput.setDegreeOfParallelism(noSubTasks);

		MapContract<NodePair, Path, PactString, Path> pathStarts = new MapContract<NodePair, Path, PactString, Path>(
			ProjectPathStart.class, "Project Starts");
		pathStarts.setDegreeOfParallelism(noSubTasks);

		MapContract<NodePair, Path, PactString, Path> pathEnds = new MapContract<NodePair, Path, PactString, Path>(
			ProjectPathEnd.class, "Project Ends");
		pathEnds.setDegreeOfParallelism(noSubTasks);

		MatchContract<PactString, Path, Path, NodePair, Path> concatPaths = new MatchContract<PactString, Path, Path, NodePair, Path>(
			ConcatPaths.class, "Concat Paths");
		concatPaths.setDegreeOfParallelism(noSubTasks);

		CoGroupContract<NodePair, Path, Path, PactNull, Path> findShortestPaths = new CoGroupContract<NodePair, Path, Path, PactNull, Path>(
			FindShortestPath.class, "Find Shortest Paths");
		findShortestPaths.setDegreeOfParallelism(noSubTasks);

		DataSinkContract<PactNull, Path> result = new DataSinkContract<PactNull, Path>(PathOutFormat.class,
			output);
		result.setDegreeOfParallelism(noSubTasks);

		result.setInput(findShortestPaths);
		findShortestPaths.setFirstInput(pathsInput);
		findShortestPaths.setSecondInput(concatPaths);
		concatPaths.setFirstInput(pathStarts);
		pathStarts.setInput(pathsInput);
		concatPaths.setSecondInput(pathEnds);
		pathEnds.setInput(pathsInput);

		return new Plan(result, "Pairwise Shortest Paths");

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssemblerDescription#getDescription()
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks], [inputPaths], [outputPaths], [RDFInputFlag]";
	}

}
