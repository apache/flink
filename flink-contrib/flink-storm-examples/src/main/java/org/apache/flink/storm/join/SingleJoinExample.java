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
package org.apache.flink.storm.join;

import backtype.storm.Config;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.util.BoltFileSink;
import org.apache.flink.storm.util.TupleOutputFormatter;
import storm.starter.bolt.PrinterBolt;
import storm.starter.bolt.SingleJoinBolt;


public class SingleJoinExample {

	public static void main(String[] args) throws Exception {
		final FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender", "hobbies"));
		final FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("gender", genderSpout);

		builder.setSpout("age", ageSpout);

		builder.setBolt("join", new SingleJoinBolt(new Fields("gender", "age")))
			.fieldsGrouping("gender", new Fields("id"))
			.fieldsGrouping("age", new Fields("id"));

		// emit result
		if (args.length > 0) {
        	// read the text file from given input path
			builder.setBolt("fileOutput", new BoltFileSink(args[0], new TupleOutputFormatter()))
				.shuffleGrouping("join");
		} else {
			builder.setBolt("print", new PrinterBolt()).shuffleGrouping("join");
		}

		Config conf = new Config();
		conf.setDebug(true);

		String[] hobbies = new String[] {"reading", "biking", "travelling", "watching tv"};

		for (int i = 0; i < 10; i++) {
			String gender;
			if (i % 2 == 0) {
				gender = "male";
			}
			else {
				gender = "female";
			}
			genderSpout.feed(new Values(i, gender, hobbies[i % hobbies.length]));
		}

		for (int i = 9; i >= 0; i--) {
			ageSpout.feed(new Values(i, i + 20));
		}


		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology("joinTopology", conf, FlinkTopology.createTopology(builder));

		Utils.sleep(10 * 1000);

		cluster.shutdown();

	}
}
