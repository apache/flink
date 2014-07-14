/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.test.cellinfo;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.log4j.Level;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.JobWithJars;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.LogUtils;

public class CellInfo {

	protected final Configuration config;
	
	public CellInfo() {
		this(new Configuration());
	}

	public CellInfo(Configuration config) {
		this.config = config;

		LogUtils.initializeDefaultConsoleLogger(Level.WARN);
	}
	
	
  public static JobGraph getJobGraph() {
    JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
    graphBuilder.setSource("infoSource", InfoSourceInvokable.class);
//    graphBuilder.setSource("infoSource2", InfoSourceInvokable.class);
    graphBuilder.setSource("querySource", QuerySourceInvokable.class);
//    graphBuilder.setSource("querySource2", QuerySourceInvokable.class);
    graphBuilder.setTask("cellTask", CellTaskInvokable.class, 3);
    graphBuilder.setSink("sink", CellSinkInvokable.class);
    
    graphBuilder.fieldsConnect("infoSource", "cellTask", 0, IntValue.class);
    graphBuilder.fieldsConnect("querySource", "cellTask",0, IntValue.class);
//    graphBuilder.fieldsConnect("infoSource2", "cellTask", 0, IntValue.class);
//    graphBuilder.fieldsConnect("querySource2", "cellTask",0, IntValue.class);
    graphBuilder.shuffleConnect("cellTask", "sink");

    return graphBuilder.getJobGraph();
  }
  
  public static void main(String[] args) {

  	NepheleMiniCluster exec = new NepheleMiniCluster();
		try {

			File file = new File("target/stratosphere-streaming-0.5-SNAPSHOT.jar");
			JobWithJars.checkJarFile(file);

			JobGraph jG = getJobGraph();

			jG.addJar(new Path(file.getAbsolutePath()));

			Configuration configuration = jG.getJobConfiguration();
			
//			Client client = new Client(new InetSocketAddress("localhost", 6498),
//					configuration);

			Client client = new Client(new InetSocketAddress(
					"hadoop02.ilab.sztaki.hu", 6123), configuration);
			exec.start();
			client.run(null, jG, true);
			exec.stop();
		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
