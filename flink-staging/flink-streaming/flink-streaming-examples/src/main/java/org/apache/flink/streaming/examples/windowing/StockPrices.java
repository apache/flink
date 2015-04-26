/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

/**
 * This example showcases a moderately complex Flink Streaming pipeline.
 * It to computes statistics on stock market data that arrive continuously,
 * and combines the stock market data with tweet streams.
 * For a detailed explanation of the job, check out the blog post unrolling it.
 * To run the example make sure that the service providing the text data
 * is already up and running.
 *
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 *
 *
 * <p>
 * Usage:
 * <code>StockPrices &lt;hostname&gt; &lt;port&gt; &lt;result path&gt;</code>
 * <br>
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>merge and join data streams,
 * <li>use different windowing policies,
 * <li>define windowing aggregations.
 * </ul>
 *
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 * @see <a href="http://flink.apache.org/news/2015/02/09/streaming-example.html">blogpost</a>
 */
public class StockPrices {

	private static final ArrayList<String> SYMBOLS = new ArrayList<String>(Arrays.asList("SPX", "FTSE", "DJI", "DJT", "BUX", "DAX", "GOOG"));
	private static final Double DEFAULT_PRICE = 1000.;
	private static final StockPrice DEFAULT_STOCK_PRICE = new StockPrice("", DEFAULT_PRICE);

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	@SuppressWarnings({ "serial", "unused" })
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//Step 1 
	    //Read a stream of stock prices from different sources and merge it into one stream
		
		//Read from a socket stream at map it to StockPrice objects
		DataStream<StockPrice> socketStockStream = env.socketTextStream(hostName, port)
				.map(new MapFunction<String, StockPrice>() {
					private String[] tokens;

					@Override
					public StockPrice map(String value) throws Exception {
						tokens = value.split(",");
						return new StockPrice(tokens[0], Double.parseDouble(tokens[1]));
					}
				});

		//Generate other stock streams
		DataStream<StockPrice> SPX_stream = env.addSource(new StockSource("SPX", 10));
		DataStream<StockPrice> FTSE_stream = env.addSource(new StockSource("FTSE", 20));
		DataStream<StockPrice> DJI_stream = env.addSource(new StockSource("DJI", 30));
		DataStream<StockPrice> BUX_stream = env.addSource(new StockSource("BUX", 40));

		//Merge all stock streams together
		@SuppressWarnings("unchecked")
		DataStream<StockPrice> stockStream = socketStockStream.merge(SPX_stream, FTSE_stream, DJI_stream, BUX_stream);
		
		//Step 2
	    //Compute some simple statistics on a rolling window
		WindowedDataStream<StockPrice> windowedStream = stockStream
				.window(Time.of(10, TimeUnit.SECONDS))
				.every(Time.of(5, TimeUnit.SECONDS));

		DataStream<StockPrice> lowest = windowedStream.minBy("price").flatten();
		DataStream<StockPrice> maxByStock = windowedStream.groupBy("symbol").maxBy("price").flatten();
		DataStream<StockPrice> rollingMean = windowedStream.groupBy("symbol").mapWindow(new WindowMean()).flatten();

		//Step 3
		//Use  delta policy to create price change warnings, and also count the number of warning every half minute

		DataStream<String> priceWarnings = stockStream.groupBy("symbol")
				.window(Delta.of(0.05, new DeltaFunction<StockPrice>() {
					@Override
					public double getDelta(StockPrice oldDataPoint, StockPrice newDataPoint) {
						return Math.abs(oldDataPoint.price - newDataPoint.price);
					}
				}, DEFAULT_STOCK_PRICE))
				.mapWindow(new SendWarning()).flatten();


		DataStream<Count> warningsPerStock = priceWarnings.map(new MapFunction<String, Count>() {
			@Override
			public Count map(String value) throws Exception {
				return new Count(value, 1);
			}
		}).groupBy("symbol").window(Time.of(30, TimeUnit.SECONDS)).sum("count").flatten();

		//Step 4
		//Read a stream of tweets and extract the stock symbols
		DataStream<String> tweetStream = env.addSource(new TweetSource());

		DataStream<String> mentionedSymbols = tweetStream.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				String[] words = value.split(" ");
				for (String word : words) {
					out.collect(word.toUpperCase());
				}
			}
		}).filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return SYMBOLS.contains(value);
			}
		});

		DataStream<Count> tweetsPerStock = mentionedSymbols.map(new MapFunction<String, Count>() {
			@Override
			public Count map(String value) throws Exception {
				return new Count(value, 1);
			}
		}).groupBy("symbol")
				.window(Time.of(30, TimeUnit.SECONDS))
				.sum("count").flatten();

		//Step 5
		//For advanced analysis we join the number of tweets and the number of price change warnings by stock
		//for the last half minute, we keep only the counts. We use this information to compute rolling correlations
		//between the tweets and the price changes

		DataStream<Tuple2<Integer, Integer>> tweetsAndWarning = warningsPerStock.join(tweetsPerStock)
				.onWindow(30, TimeUnit.SECONDS)
				.where("symbol")
				.equalTo("symbol")
				.with(new JoinFunction<Count, Count, Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<Integer, Integer> join(Count first, Count second) throws Exception {
						return new Tuple2<Integer, Integer>(first.count, second.count);
					}
				});

		DataStream<Double> rollingCorrelation = tweetsAndWarning
				.window(Time.of(30, TimeUnit.SECONDS))
				.mapWindow(new WindowCorrelation()).flatten();

		if (fileOutput) {
			rollingCorrelation.writeAsText(outputPath, 1);
		} else {
			rollingCorrelation.print();
		}

		env.execute("Stock stream");

	}

	// *************************************************************************
	// DATA TYPES
	// *************************************************************************

	public static class StockPrice implements Serializable {

		private static final long serialVersionUID = 1L;
		public String symbol;
		public Double price;

		public StockPrice() {
		}

		public StockPrice(String symbol, Double price) {
			this.symbol = symbol;
			this.price = price;
		}

		@Override
		public String toString() {
			return "StockPrice{" +
					"symbol='" + symbol + '\'' +
					", count=" + price +
					'}';
		}
	}

	public static class Count implements Serializable{
		
		private static final long serialVersionUID = 1L;
		public String symbol;
		public Integer count;

		public Count() {
		}

		public Count(String symbol, Integer count) {
			this.symbol = symbol;
			this.count = count;
		}

		@Override
		public String toString() {
			return "Count{" +
					"symbol='" + symbol + '\'' +
					", count=" + count +
					'}';
		}
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public final static class StockSource implements SourceFunction<StockPrice> {

		private static final long serialVersionUID = 1L;
		private Double price;
		private String symbol;
		private Integer sigma;

		public StockSource(String symbol, Integer sigma) {
			this.symbol = symbol;
			this.sigma = sigma;
		}

		@Override
		public void run(Collector<StockPrice> collector) throws Exception {
			price = DEFAULT_PRICE;
			Random random = new Random();

			while (true) {
				price = price + random.nextGaussian() * sigma;
				collector.collect(new StockPrice(symbol, price));
				Thread.sleep(random.nextInt(200));
			}
		}
		
		@Override
		public void cancel() {
			// No cleanup needed
		}
	}

	public final static class WindowMean implements WindowMapFunction<StockPrice, StockPrice> {

		private static final long serialVersionUID = 1L;
		private Double sum = 0.0;
		private Integer count = 0;
		private String symbol = "";

		@Override
		public void mapWindow(Iterable<StockPrice> values, Collector<StockPrice> out) throws Exception {
			if (values.iterator().hasNext()) {

				for (StockPrice sp : values) {
					sum += sp.price;
					symbol = sp.symbol;
					count++;
				}
				out.collect(new StockPrice(symbol, sum / count));
			}
		}
	}

	public static final class TweetSource implements SourceFunction<String> {

		private static final long serialVersionUID = 1L;
		Random random;
		StringBuilder stringBuilder;

		@Override
		public void run(Collector<String> collector) throws Exception {
			random = new Random();
			stringBuilder = new StringBuilder();

			while (true) {
				stringBuilder.setLength(0);
				for (int i = 0; i < 3; i++) {
					stringBuilder.append(" ");
					stringBuilder.append(SYMBOLS.get(random.nextInt(SYMBOLS.size())));
				}
				collector.collect(stringBuilder.toString());
				Thread.sleep(500);
			}

		}
		
		@Override
		public void cancel() {
			// No cleanup needed
		}
	}

	public static final class SendWarning implements WindowMapFunction<StockPrice, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void mapWindow(Iterable<StockPrice> values, Collector<String> out) throws Exception {
			if (values.iterator().hasNext()) {
				out.collect(values.iterator().next().symbol);
			}
		}
	}

	public static final class WindowCorrelation implements WindowMapFunction<Tuple2<Integer, Integer>, Double> {

		private static final long serialVersionUID = 1L;
		private Integer leftSum;
		private Integer rightSum;
		private Integer count;

		private Double leftMean;
		private Double rightMean;

		private Double cov;
		private Double leftSd;
		private Double rightSd;

		@Override
		public void mapWindow(Iterable<Tuple2<Integer, Integer>> values, Collector<Double> out) throws Exception {

			leftSum = 0;
			rightSum = 0;
			count = 0;

			cov = 0.;
			leftSd = 0.;
			rightSd = 0.;

			//compute mean for both sides, save count
			for (Tuple2<Integer, Integer> pair : values) {
				leftSum += pair.f0;
				rightSum += pair.f1;
				count++;
			}

			leftMean = leftSum.doubleValue() / count;
			rightMean = rightSum.doubleValue() / count;

			//compute covariance & std. deviations
			for (Tuple2<Integer, Integer> pair : values) {
				cov += (pair.f0 - leftMean) * (pair.f1 - rightMean) / count;
			}

			for (Tuple2<Integer, Integer> pair : values) {
				leftSd += Math.pow(pair.f0 - leftMean, 2) / count;
				rightSd += Math.pow(pair.f1 - rightMean, 2) / count;
			}
			leftSd = Math.sqrt(leftSd);
			rightSd = Math.sqrt(rightSd);

			out.collect(cov / (leftSd * rightSd));
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String hostName;
	private static int port;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		// parse input arguments
		if (args.length == 3) {
			fileOutput = true;
			hostName = args[0];
			port = Integer.valueOf(args[1]);
			outputPath = args[2];
		} else if (args.length == 2) {
			hostName = args[0];
			port = Integer.valueOf(args[1]);
		} else {
			System.err.println("Usage: StockPrices <hostname> <port> [<output path>]");
			return false;
		}
		return true;
	}

}
