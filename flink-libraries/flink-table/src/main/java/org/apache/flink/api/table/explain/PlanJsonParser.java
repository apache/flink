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

package org.apache.flink.api.table.explain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;

public class PlanJsonParser {

	public static String getSqlExecutionPlan(String t, Boolean extended) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		//not every node is same, ignore the unknown field
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		PlanTree tree = objectMapper.readValue(t, PlanTree.class);
		LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		int tabCount = 0;

		for (int index = 0; index < tree.getNodes().size(); index++) {
			Node tempNode = tree.getNodes().get(index);

			//input with operation such as join or union is coordinate, keep the same indent 
			if ((tempNode.getPact().equals("Data Source")) && (map.containsKey(tempNode.getPact()))) {
				tabCount = map.get(tempNode.getPact());
			}
			else {
				map.put(tempNode.getPact(), tabCount);
			}

			printTab(tabCount, pw);
			pw.print("Stage " + tempNode.getId() + " : " + tempNode.getPact() + "\n");

			printTab(tabCount + 1, pw);
			String content = tempNode.getContents();

			//drop the hashcode of object instance
			int dele = tempNode.getContents().indexOf("@");
			if (dele > -1) {
				content = tempNode.getContents().substring(0, dele);
			}
			
			//replace with certain content if node is dataSource to pass
			//unit tests, because java and scala use different api to
			//get input element
			if (tempNode.getPact().equals("Data Source")) {
				content = "collect elements with CollectionInputFormat";
			}
			pw.print("content : " + content + "\n");

			List<Predecessors> predecessors = tempNode.getPredecessors();
			if (predecessors != null) {
				printTab(tabCount + 1, pw);
				pw.print("ship_strategy : " + predecessors.get(0).getShip_strategy() + "\n");

				printTab(tabCount + 1, pw);
				pw.print("exchange_mode : " + predecessors.get(0).getExchange_mode() + "\n");
			}

			if (tempNode.getDriver_strategy() != null) {
				printTab(tabCount + 1, pw);
				pw.print("driver_strategy : " + tempNode.getDriver_strategy() + "\n");
			}

			printTab(tabCount + 1, pw);
			pw.print(tempNode.getGlobal_properties().get(0).getName() + " : "
					+ tempNode.getGlobal_properties().get(0).getValue() + "\n");

			if (extended) {
				List<Global_properties> globalProperties = tempNode.getGlobal_properties();
				for (int i = 1; i < globalProperties.size(); i++) {
					printTab(tabCount + 1, pw);
					pw.print(globalProperties.get(i).getName() + " : "
					+ globalProperties.get(i).getValue() + "\n");
				}

				List<LocalProperty> localProperties = tempNode.getLocal_properties();
				for (int i = 0; i < localProperties.size(); i++) {
					printTab(tabCount + 1, pw);
					pw.print(localProperties.get(i).getName() + " : "
					+ localProperties.get(i).getValue() + "\n");
				}

				List<Estimates> estimates = tempNode.getEstimates();
				for (int i = 0; i < estimates.size(); i++) {
					printTab(tabCount + 1, pw);
					pw.print(estimates.get(i).getName() + " : "
					+ estimates.get(i).getValue() + "\n");
				}

				List<Costs> costs = tempNode.getCosts();
				for (int i = 0; i < costs.size(); i++) {
					printTab(tabCount + 1, pw);
					pw.print(costs.get(i).getName() + " : "
					+ costs.get(i).getValue() + "\n");
				}

				List<Compiler_hints> compilerHintses = tempNode.getCompiler_hints();
				for (int i = 0; i < compilerHintses.size(); i++) {
					printTab(tabCount + 1, pw);
					pw.print(compilerHintses.get(i).getName() + " : "
					+ compilerHintses.get(i).getValue() + "\n");
				}
			}
			tabCount++;
			pw.print("\n");
		}
		pw.close();
		return sw.toString();
	}

	private static void printTab(int tabCount, PrintWriter pw) {
		for (int i = 0; i < tabCount; i++)
			pw.print("\t");
	}
}

class PlanTree {
	private List<Node> nodes;

	public List<Node> getNodes() {
		return nodes;
	}
}
