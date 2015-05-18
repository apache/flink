package org.apache.flink.streaming.connectors.twitter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.util.Collector;

public class TwitterFilterSourceTest {

	private static final String PATH_TO_AUTH_FILE = "/twitter.properties";
	
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		
		TwitterFilterSource twitterSource = new TwitterFilterSource(TwitterFilterSourceTest.class.getResource(PATH_TO_AUTH_FILE).getFile());

		twitterSource.trackTerm("obama");
		twitterSource.filterLanguage("en");
		
		DataStream<String> streamSource = env.addSource(twitterSource).flatMap(new JSONParseFlatMap<String, String>() {

			@Override
			public void flatMap(String s, Collector<String> c)
					throws Exception {
				c.collect(s);
			}
		});

		streamSource.print();
		
		try {
			env.execute("Twitter Streaming Test");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}