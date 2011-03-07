package eu.stratosphere.pact.runtime.histogram;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class HistogramStub
	extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {
	
	int buckets = 10;

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		//TODO: Make buckets configurable
	}

	@Override
	/** Assumes values are sorted */
	public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
		int count = key.getValue();
		int bucketSize = count / buckets;
		
		for (int i = 0; i < count; i++) {
			PactInteger value = values.next();
			
			if(i%bucketSize == 0 && i/bucketSize != 0 && i/bucketSize != buckets) {
				out.collect(new PactInteger(i/bucketSize), value);
			}
		}
	}

	public static void main(String[] args) {
		HistogramStub histo = new HistogramStub();
		ArrayList<PactInteger> values = new ArrayList<PactInteger>();
		
		ArrayList<KeyValuePair<PactInteger, PactInteger>> results = new ArrayList<KeyValuePair<PactInteger, PactInteger>>();
		Collector<PactInteger, PactInteger> collector = new ListCollector<PactInteger, PactInteger>(results);
		int num = 1005;
		
		for (int i = 0; i < num; i++) {
			values.add(new PactInteger(i));
		}
		
		histo.reduce(new PactInteger(num), values.iterator(), collector);
	}
	
	/**
	 * A simple collector that collects Key and Value and puts them into an <tt>ArrayList</tt>.
	 */
	private static final class ListCollector<K extends Key, V extends Value> implements Collector<K, V> {
		private ArrayList<KeyValuePair<K, V>> list; // the list to collect pairs in

		/**
		 * Creates a new collector that collects output in the given list.
		 * 
		 * @param list
		 *        The list to collect output in.
		 */
		private ListCollector(ArrayList<KeyValuePair<K, V>> list) {
			this.list = list;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key,
		 * eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(K key, V value) {
			list.add(new KeyValuePair<K, V>(key, value));

		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#close()
		 */
		@Override
		public void close() {
			// does nothing
		}

	}
}
