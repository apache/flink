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

package eu.stratosphere.pact.example.graph.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Generates graphs keeping record of edges and triangles that are created.
 */
public class EnumTrianglesGenerator {

	private static final Log LOG = LogFactory.getLog(EnumTrianglesGenerator.class);

	/** Seed for the random generator (allows regenerating the same data) */
	private static long seed = 0;

	/**
	 * Maximum number of retries to pick a first node (and a second for each
	 * first node picked).
	 */
	private static final int MAX_TRIES = 5;

	/** Maximum number of nodes in a graph before it needs to be dumped to file. */
	private static int MAX_NODES_PER_GRAPH = 100000;

	/** Size of the buffer to use by the BufferedWriter. */
	private static final int WRITE_BUFFER_SIZE = 1024 * 1024 * 5;

	/** Path to output file name for edges. */
	private String outputFileEdges;

	/** Path to output file name for triangle. */
	private String outputFileTriangles;

	/** FileWriter to output data to files. */
	BufferedWriter bwEdges;

	BufferedWriter bwTriangs;

	/** Maximum number of bytes to write to a single file. */
	private int maxFileSize;

	/** Current number for edges output file. */
	private int curFileEdges = 0;

	/** Current number for triangs output file. */
	private int curFileTriangs = 0;

	/** Current counter of written bytes to edges output file. */
	private int curBufferEdges = 0;

	/** Current counter of written bytes to triangs output file. */
	private int curBufferTriangs = 0;

	/**
	 * Very simple name generator class
	 */
	private static class NameGenerator {

		private static int cnt = 0;

		public static String getNewName() {

			return "N" + NameGenerator.cnt++;

		}
	}

	/**
	 * Ctr.
	 * 
	 * @param outputEdges
	 *        path to file to write edges representations to
	 * @param outputTriangs
	 *        path to file to write triangs representations to
	 * @param maxFileSize
	 *        maximum number of bytes to write to a file before splitting
	 */
	public EnumTrianglesGenerator(String outputEdges, String outputTriangs, int maxFileSize) {

		this.outputFileEdges = outputEdges;
		this.outputFileTriangles = outputTriangs;
		this.maxFileSize = maxFileSize;

	}

	/**
	 * Opens the file handle for the edges output file.
	 * 
	 * @throws IOException
	 */
	private void openHandleEdges() throws IOException {

		bwEdges = new BufferedWriter(new FileWriter(new File(this.outputFileEdges + this.curFileEdges)),
			WRITE_BUFFER_SIZE);

	}

	/**
	 * Closes the old file handle for the edges output file and open a new file
	 * handle.
	 */
	private void openNewHandleEdges() throws IOException {

		bwEdges.flush();
		bwEdges.close();
		bwEdges = new BufferedWriter(new FileWriter(new File(this.outputFileEdges + ++this.curFileEdges)),
			WRITE_BUFFER_SIZE);

	}

	/**
	 * Closes the file handle for the edges output file.
	 * 
	 * @throws IOException
	 */
	private void closeHandleEdges() throws IOException {

		bwEdges.flush();
		bwEdges.close();
		this.curFileEdges++;

	}

	/**
	 * Opens the file handle for the triangle output file.
	 * 
	 * @throws IOException
	 */
	private void openHandleTriangs() throws IOException {

		bwTriangs = new BufferedWriter(new FileWriter(new File(this.outputFileTriangles + this.curFileTriangs)),
			WRITE_BUFFER_SIZE);

	}

	/**
	 * Closes the old file handle for the triangle output file and open a new
	 * file handle.
	 * 
	 * @throws IOException
	 */
	private void openNewHandleTriangs() throws IOException {

		bwTriangs.flush();
		bwTriangs.close();
		bwTriangs = new BufferedWriter(new FileWriter(new File(this.outputFileTriangles + ++this.curFileTriangs)),
			WRITE_BUFFER_SIZE);

	}

	/**
	 * Closes the file handle for the triangle output file.
	 * 
	 * @throws IOException
	 */
	private void closeHandleTriangs() throws IOException {

		bwTriangs.flush();
		bwTriangs.close();
		this.curFileTriangs++;

	}

	/**
	 * Writes an edge string representation to the output file.
	 * 
	 * @param edge
	 *        string representation of an edge
	 * @throws IOException
	 */
	protected void writeEdge(String edge) throws IOException {

		// check if file will not exceed maximum size
		if (this.curBufferEdges + edge.length() + 1 > this.maxFileSize) {

			// open new file
			this.openNewHandleEdges();
			// reset buffer counter
			this.curBufferEdges = 0;

		}

		// write triangle representation to output
		bwEdges.write(edge + "\n");
		this.curBufferEdges += edge.length() + 1;

	}

	/**
	 * Writes the given triangle representations to the corresponding output.
	 * 
	 * @param triangles
	 *        Collection of triangle representations
	 */
	protected void writeTriangles(Collection<String> triangles) throws IOException {

		for (String triang : triangles) {

			// check if file will not exceed maximum size
			if (this.curBufferTriangs + triang.length() + 1 > this.maxFileSize) {

				// open new file
				this.openNewHandleTriangs();
				// reset buffer counter
				this.curBufferTriangs = 0;

			}

			// write triangle representation to output
			bwTriangs.write(triang + "\n");
			this.curBufferTriangs += triang.length() + 1;

		}

	}

	/**
	 * Generates a graph with the given number of nodes and edges.
	 * 
	 * @param nodeCount
	 *        number of nodes in the graph
	 * @param edgeCount
	 *        additional number of edges in the graph (apart from the edges
	 *        needed to connect the all nodes)
	 * @return constructed graph with information about triangles
	 */
	protected TriangGraph generateGraph(int nodeCount, int edgeCount) throws IOException {

		TriangGraph graph = new TriangGraph();

		// remember last node to make at least a weak connection
		TriangNode lastNode = null;

		// generate required number of nodes
		for (int i = 0; i < nodeCount; i++) {

			// create node in the graph
			TriangNode node = new TriangNode(NameGenerator.getNewName(), graph);
			graph.addNode(node);
			// create a minimally connected graph
			if (lastNode != null) {
				lastNode.addEdge(node);
				this.writeEdge(TriangNode.getEdgeString(node, lastNode));
			}
			lastNode = node;

		}

		LOG.info("Generated " + nodeCount + " nodes.");

		// seed generator to be able to regenerate the same data
		Random rnd = new Random(EnumTrianglesGenerator.seed);
		for (int i = 0; i < edgeCount; i++) {

			if (i % 1000000 == 0 && i != 0)
				LOG.info("Generated " + (i / 1000000) + ".000.000 edges.");

			// pick randomly any nodes indexes
			int index1 = rnd.nextInt(graph.getNodeCount());
			int index2 = rnd.nextInt(graph.getNodeCount());
			if (index2 == index1) {

				if (index2 < graph.getNodeCount() - 1)
					index2++;
				else
					index2--;

			}

			// get nodes at index positions
			TriangNode node1 = graph.getNode(index1);
			TriangNode node2 = graph.getNode(index2);

			// try to change the first node if switching the second node
			// accomplished nothing
			int triesFirstNode = 0;
			boolean abort = false;
			while (!abort && triesFirstNode < EnumTrianglesGenerator.MAX_TRIES) {

				// count the number of tries to connect the first node with
				// another node
				int triesSecondNode = 0;

				// check whether nodes are connected and pick new nodes to avoid
				while (node1.hasEdgeTo(node2) && triesSecondNode < EnumTrianglesGenerator.MAX_TRIES) {

					// get next node
					index2 = rnd.nextInt(graph.getNodeCount());
					// erase the chance that a self loop edge can be created
					if (index2 == index1) {

						if (index2 < graph.getNodeCount() - 1)
							index2++;
						else
							index2--;

					}

					node2 = graph.getNode(index2);

					triesSecondNode++;

				}

				// if node1 has an edge to node2 adding an edge failed
				if (node1.hasEdgeTo(node2)) {

					triesFirstNode++;
					// get new node1 to try again
					index1 = rnd.nextInt(graph.getNodeCount());
					node1 = graph.getNode(index1);

				} else {

					// abort search for two unconnected nodes
					abort = true;

				}

			}

			if (triesFirstNode < EnumTrianglesGenerator.MAX_TRIES) {

				// connect nodes
				node1.addEdge(node2);
				this.writeEdge(TriangNode.getEdgeString(node1, node2));

				// calculate possibly generated triangles
				Collection<String> triangStrings = graph.calculateTriangles(node1, node2);
				this.writeTriangles(triangStrings);

			} else {

				LOG
					.debug("Exceeded number of retries to connect two randomly chosen nodes in the graph. No edge was added");

			}

		}

		return graph;

	}

	/**
	 * Function to generate a graph with triangles and record them.
	 * 
	 * @param nodeCount
	 *        number of nodes in the final graph
	 * @param edgeCount
	 *        number of edges in the final graph, eges > nodes is essential
	 */
	public void generateTriangles(long nodeCount, long edgeCount) throws IOException {

		LOG.info("Starting processing...");

		// open file handles to write data
		this.openHandleEdges();
		this.openHandleTriangs();

		// subtract edges to at least connect all nodes in the graph
		edgeCount -= nodeCount - 1;
		// calculate number of subgraphs to generate to avoid running out of
		// memory
		int numberOfGraphs = (int) Math.ceil(nodeCount / MAX_NODES_PER_GRAPH);
		if (numberOfGraphs < 1)
			numberOfGraphs = 1;

		// calculate number of edges per full graph
		int numberOfEdgesPerMaxGraph = (int) Math.floor((((numberOfGraphs - 1) * MAX_NODES_PER_GRAPH) / nodeCount)
			* edgeCount);
		// calculate number of remaining edges
		long remainingEdges = edgeCount - (numberOfEdgesPerMaxGraph * (numberOfGraphs - 1));

		String lastNodeName = null;
		long curNodeCount = nodeCount;
		while (curNodeCount > MAX_NODES_PER_GRAPH) {

			curNodeCount -= MAX_NODES_PER_GRAPH;
			TriangGraph graph = this.generateGraph(MAX_NODES_PER_GRAPH, numberOfEdgesPerMaxGraph);

			// pick a random node of the current graph
			String currentNodeName = graph.getNode(graph.getNodeCount() - 1).getName();

			// if previous node name is set create an edge between the graphs
			if (lastNodeName != null)
				this.writeEdge(TriangNode.getEdgeString(currentNodeName, lastNodeName));

			// remember the name of a node of the last graph to wire them
			lastNodeName = currentNodeName;

		}

		// generate last graph
		TriangGraph graph = this.generateGraph((int) curNodeCount, (int) remainingEdges);
		if (lastNodeName != null)
			this.writeEdge(TriangNode.getEdgeString(graph.getNode(graph.getNodeCount() - 1).getName(), lastNodeName));

		// close file handles
		this.closeHandleEdges();
		this.closeHandleTriangs();

	}

	/**
	 * Run the generator.
	 */
	public static void main(String[] args) {

		// generator settings
		// output prefix for files storing edge information
		String outputEdges = "/home/msaecker/workspace_java/enumTriangs/edges_";
		// output prefix for files storing triangle information
		String outputTriangles = "/home/msaecker/workspace_java/enumTriangs/triangs_";
		// maximum size of a file (output will be split off into a new file)
		int maxSizePerFile = 1024 * 1024 * 120;

		// number of nodes in the graph
		long nodeCount = 1000;
		// maximum number of nodes in a subgraph before dumping it to the file
		EnumTrianglesGenerator.MAX_NODES_PER_GRAPH = 100000;

		// calculate maximum number of edges in a graph of this size
		long fullConnectedEdgesCount = 0;
		for (long i = 1; i < nodeCount; i++) {

			fullConnectedEdgesCount += i - 1;

		}

		// define edge count by ratio of fully connected graph (at least
		// nodeCount edges are necessary, elsewhise not all nodes can be
		// connected to the graph)
		long edgeCount = nodeCount + fullConnectedEdgesCount / 8;

		// print information about the graph
		LOG.info("Number of edges for fully connected graph: " + fullConnectedEdgesCount);
		LOG.info("Number of edges in the graph: " + edgeCount);
		LOG.info("Filling degree: " + ((float) edgeCount / fullConnectedEdgesCount) * 100 + "%");

		EnumTrianglesGenerator generator = new EnumTrianglesGenerator(outputEdges, outputTriangles, maxSizePerFile);

		try {

			// generate graph
			generator.generateTriangles(nodeCount, edgeCount);

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

}
