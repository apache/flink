package storm.starter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import storm.starter.spout.RandomSentenceSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

	public static class HamletSpout extends BaseRichSpout {
		SpoutOutputCollector _collector;
		final static String path = "/home/hermann/asd.txt";
		final static int emitHamlets = 5;

		BufferedReader br = null;
		int numberOfHamlets;

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
			try {
				br = new BufferedReader(new FileReader(path));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
	
			numberOfHamlets = 0;
		}

		@Override
		public void nextTuple() {
			Utils.sleep(100);
			String line = "";
			line = getLine();

			if (line != null) {
				_collector.emit(new Values(line));
			}
		}

		public String getLine() {
			String line;
			try {
				line = br.readLine();
			if (line == null && numberOfHamlets < emitHamlets) {
				numberOfHamlets++;
				br = new BufferedReader(new FileReader(path));
				line = br.readLine();
			}
			return line;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public void ack(Object id) {
		}

		@Override
		public void fail(Object id) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	public static class SplitSentence extends ShellBolt implements IRichBolt {

		public SplitSentence() {
			super("python", "splitsentence.py");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}

	public static class WordCount extends BaseBasicBolt {
		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new HamletSpout(), 1);

		builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
		builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();
		}
	}
}
