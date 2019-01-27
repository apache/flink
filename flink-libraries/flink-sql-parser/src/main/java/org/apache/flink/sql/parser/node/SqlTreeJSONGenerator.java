/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.node;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON generator for sql tree nodes.
 */
public class SqlTreeJSONGenerator {

	private int idCounter = 1;
	private Map<SqlTreeNode, Node> nodeMap = new HashMap<>();
	private List<Link> links = new ArrayList<>();

	public String getJSON(List<SqlTreeNode> treeNodes) {

		for (SqlTreeNode treeNode : treeNodes) {
			createNodes(treeNode);
		}

		List<Node> nodes = Lists.newArrayList(nodeMap.values());
		nodes.sort(new NodeComparator());

		Plan plan = new Plan(nodes, links);

		return plan.toString();
	}

	private Node createNodes(SqlTreeNode treeNode) {
		Node node = nodeMap.get(treeNode);
		if (node == null) {
			node = Node.of(idCounter++, treeNode);
			nodeMap.put(treeNode, node);
			for (SqlTreeNode input : treeNode.getInputs()) {
				Node inputNode = createNodes(input);
				links.add(Link.of(inputNode, node));
			}
		}
		return node;
	}

	private static String toJson(Object value) {
		try {
			return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(value);
		} catch (JsonProcessingException e) {
			throw new AssertionError("Writing a valid object as JSON string should not fail: " + e.getMessage());
		}
	}

	private static class NodeComparator implements Comparator<Node> {

		@Override
		public int compare(Node node1, Node node2) {
			return node1.id - node2.id;
		}
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private static class Plan {
		@JsonProperty("nodes")
		private List<Node> nodes;

		@JsonProperty("links")
		private List<Link> links;

		public Plan() {
		}

		Plan(List<Node> nodes, List<Link> links) {
			this.nodes = nodes;
			this.links = links;
		}

		@Override
		public String toString() {
			return toJson(this);
		}
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private static class Node {

		@JsonProperty("id")
		private int id;

		@JsonProperty("type")
		private String type;

		@JsonProperty("name")
		private String name;

		@JsonProperty("pos")
		private Position pos;

		public Node() {
		}

		Node(int id, String type, String name, Position pos) {
			this.id = id;
			this.type = type;
			this.name = name;
			this.pos = pos;
		}

		private static Node of(int idCounter, SqlTreeNode treeNode) {

			return new Node(
				idCounter,
				treeNode.getNodeType().toString(),
				treeNode.explain(),
				new Position(
					treeNode.getParserPosition().getLineNum(),
					treeNode.getParserPosition().getColumnNum()));
		}

		@Override
		public String toString() {
			return toJson(this);
		}
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private static class Position {

		@JsonProperty("line")
		private int line;

		@JsonProperty("column")
		private int column;

		public Position() {
		}

		public Position(int line, int column) {
			this.line = line;
			this.column = column;
		}

		@Override
		public String toString() {
			return toJson(this);
		}
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private static class Link {

		@JsonProperty("source")
		private int source;

		@JsonProperty("target")
		private int target;

		public Link() {
		}

		Link(int source, int target) {
			this.source = source;
			this.target = target;
		}

		private static Link of(Node source, Node target) {
			return new Link(source.id, target.id);
		}

		@Override
		public String toString() {
			return toJson(this);
		}
	}
}
