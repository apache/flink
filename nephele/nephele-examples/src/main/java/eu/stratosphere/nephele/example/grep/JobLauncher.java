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

package eu.stratosphere.nephele.example.grep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;

public class JobLauncher {

	public static void main(String[] args) {

		List<String> pars = Arrays.asList(args);

		int lengthPars = pars.size();
		int posJar = pars.indexOf("-jar");
		int posConf = pars.indexOf("-conf");

		String jobName = "";
		List<String> jars = new ArrayList<String>();
		List<String> confs = new ArrayList<String>();

		if (posJar == -1 && posConf == -1) {
			if (lengthPars != 1) {
				System.err.println("Usage: JobLauncher JobName -jar <list of jars> -conf <list of confs>");
				System.exit(1);
			} else {
				jobName = pars.get(0);
			}
		} else if (posJar != -1 && posConf == -1) {
			if (posJar != 1 || lengthPars < 3) {
				System.err.println("Usage: JobLauncher JobName -jar <list of jars> -conf <list of confs>");
				System.exit(1);
			} else {
				jobName = pars.get(0);
				jars = pars.subList(2, pars.size());
			}
		} else if (posJar == -1 && posConf != -1) {
			if (posConf != 1 || lengthPars < 3) {
				System.err.println("Usage: JobLauncher JobName -jar <list of jars> -conf <list of confs>");
				System.exit(1);
			} else {
				jobName = pars.get(0);
				confs = pars.subList(posConf + 1, pars.size());
			}
		} else {
			if (pars.size() < 5 || posJar != 1 || posConf < 3 || posConf == pars.size() - 1) {
				System.err.println("Usage: JobLauncher JobName -jar <list of jars> -conf <list of confs>");
				System.exit(1);
			} else {
				jobName = pars.get(0);
				jars = pars.subList(2, posConf);
				confs = pars.subList(posConf + 1, pars.size());
			}
		}

		try {

			Job job = (Job) Class.forName(jobName).newInstance();
			JobGraph jobGraph = job.getJobGraph();

			for (int i = 0; i < jars.size(); i++) {
				jobGraph.addJar(new Path(jars.get(i)));
			}

			Configuration jobConfiguration = jobGraph.getJobConfiguration();
			Configuration clientConfiguration = new Configuration();

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

			DocumentBuilder db = dbf.newDocumentBuilder();

			for (int i = 0; i < confs.size(); i++) {

				Document doc = db.parse(confs.get(i));
				NodeList nl = doc.getElementsByTagName("property");

				for (int j = 0; j < nl.getLength(); j++) {

					Element property = (Element) nl.item(j);
					Node nodeKey = property.getElementsByTagName("key").item(0);
					Node nodeValue = property.getElementsByTagName("value").item(0);
					String key = nodeKey.getFirstChild().getNodeValue();
					String value = nodeValue.getFirstChild().getNodeValue();

					if (key.startsWith("job.")) {
						jobConfiguration.setString(key, value);
					} else {
						clientConfiguration.setString(key, value);
					}
				}
			}

			JobClient jobClient = new JobClient(jobGraph, clientConfiguration);
			jobClient.submitJobAndWait();

		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
