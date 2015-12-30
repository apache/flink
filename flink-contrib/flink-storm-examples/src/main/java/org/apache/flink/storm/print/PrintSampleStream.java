/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.print;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import storm.starter.bolt.PrinterBolt;
import storm.starter.spout.TwitterSampleSpout;

import java.util.Arrays;

/**
 * Prints incoming tweets. Tweets can be filtered by keywords.
 */
public class PrintSampleStream {        
	public static void main(String[] args) throws Exception {
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];

		// keywords start with the 5th parameter
		String[] keyWords = Arrays.copyOfRange(args, 4, args.length);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,
								accessToken, accessTokenSecret, keyWords));
		builder.setBolt("print", new PrinterBolt())
				.shuffleGrouping("twitter");


		Config conf = new Config();

		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology("Print", conf, FlinkTopology.createTopology(builder));

		Utils.sleep(10 * 1000);

		cluster.shutdown();
	}
}
