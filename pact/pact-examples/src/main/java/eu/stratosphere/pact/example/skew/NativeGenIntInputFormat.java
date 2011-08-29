package eu.stratosphere.pact.example.skew;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.input.InputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class NativeGenIntInputFormat implements InputFormat<NativeGenInputSplit, PactInteger, PactString> {

	private final long BASE_CARDINALITY = 1000000000; // 1 Billion
	
	public static final String SCALEFACTOR_PARAMETER_KEY = "gen.input.format.scaleFactor";
	public static final String PAYLOAD_SIZE_PARAMETER_KEY = "gen.input.format.payloadSize";
	public static final String DISTFUNC_PARAMETER_KEY = "gen.input.format.distFunction";
	
	public static final String ZIPF_DISTFUNC = "zipf";
	public static final String PARETO_DISTFUNC = "pareto";
	
	private double scale;
	private IntGenerator generator;
	
	private long card;
	private long cnt;
	
	private String payLoad;
	
	@Override
	public void close() throws IOException { }


	@Override
	public void configure(Configuration parameters) {
		
		String distFunc = parameters.getString(DISTFUNC_PARAMETER_KEY, "");
		
		if(distFunc.equals("")) {
			throw new IllegalArgumentException("Distribution function must be set!");
		} else if(distFunc.equals(ZIPF_DISTFUNC)) {
			this.generator = new ZipfIntGenerator();
		} else if(distFunc.equals(PARETO_DISTFUNC)) {
			this.generator = new ParetoIntGenerator();
		}
		this.generator.initialize(parameters);
		
		this.scale = parameters.getFloat(SCALEFACTOR_PARAMETER_KEY, 1.0f);
		
		int payLoadSize = (int)parameters.getLong(PAYLOAD_SIZE_PARAMETER_KEY, 0);
		if(payLoadSize > 0) {
			char[] payLoadC = new char[payLoadSize/2];
			for(int i=0;i<payLoadC.length;i++) {
				payLoadC[i] = '.';
			}
			this.payLoad = new String(payLoadC);
		} else if(payLoadSize == 0) {
			this.payLoad = "";
		} else {
			throw new IllegalArgumentException("PayLoadSize must be >= 0");
		}
		
	}

	@Override
	public NativeGenInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		
		NativeGenInputSplit[] splits = new NativeGenInputSplit[minNumSplits];
		for(int i=0;i<minNumSplits;i++) {
			splits[i] = new NativeGenInputSplit(i+1, minNumSplits);
		}
		return splits;
		
	}

	@Override
	public KeyValuePair<PactInteger, PactString> createPair() {
		return new KeyValuePair<PactInteger, PactString>(new PactInteger(), new PactString());
	}

	@Override
	public Class<NativeGenInputSplit> getInputSplitType() {
		return NativeGenInputSplit.class;
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}

	@Override
	public boolean nextRecord(KeyValuePair<PactInteger, PactString> record)
			throws IOException {
		
		record.getKey().setValue(this.generator.nextInt());
		record.getValue().setValue(this.payLoad);
		this.cnt++;
		return true;
	}

	@Override
	public void open(NativeGenInputSplit split) throws IOException {
		this.cnt = 0;
		this.card = (long)(BASE_CARDINALITY*this.scale/split.getSplitCount());
		
		this.initializeZipf();
		
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return (cnt >= card);
	}
	
	private void initializeZipf() {
		
	}
	
	public interface IntGenerator {
		public void initialize(Configuration parameters);
		public int nextInt();
	}
	
	public static class ZipfIntGenerator implements IntGenerator {

		public static final String SKEWFACTOR_PARAMETER_KEY = "gen.input.format.skewFactor";
		public static final String MAXVAL_PARAMETER_KEY = "gen.input.format.maxVal";
		
		private double skew;
		private int maxVal;
		private double[] sumProbCache;
		private double normalizationConstant = 0.0f;
		
		@Override
		public void initialize(Configuration parameters) {

			this.skew = parameters.getFloat(SKEWFACTOR_PARAMETER_KEY, 0.0f);
			this.maxVal = (int)parameters.getLong(MAXVAL_PARAMETER_KEY, 100);
			
			// Compute normalization constant on first call only
			for (int i = 1; i <= this.maxVal; ++i) {
				this.normalizationConstant = this.normalizationConstant + (1.0 / Math.pow((double) i, this.skew));
			}
			this.normalizationConstant = 1.0 / this.normalizationConstant;
			this.sumProbCache = new double[this.maxVal];
			double sumProb = 0.0;
			for (int i = 1; i <= this.maxVal; ++i) {
				sumProb = sumProb + this.normalizationConstant / Math.pow((double) i, this.skew);
				this.sumProbCache[i - 1] = sumProb;
			}
		}

		@Override
		public int nextInt() {

			double z; // Uniform random number (0 < z < 1)
			int zipfValue = -1; // Computed exponential value to be returned

			// Pull a uniform random number (0 < z < 1)
			do {
				z = Math.random();
			} while (z == 0.0f);

			zipfValue = findRank(z, 0, this.maxVal);
			
			return zipfValue;
		}
	
		private int findRank(final double z, final int lowerBound, final int upperBound) {

			final int index = lowerBound + ((upperBound - lowerBound) / 2);
			final double sumProb = this.sumProbCache[index];

			if ((upperBound - lowerBound) <= 1) {
				return index + 1;
			}

			final double centerDiff = sumProb - z;
			// System.out.println("Center diff is " + centerDiff);
			if (centerDiff >= 0.0) {
				// sum of probabilities is too large
				return findRank(z, lowerBound, index);
			} else {
				// sum of probabilities is too small
				return findRank(z, index, upperBound);
			}
		}
	}
	
	public static class ParetoIntGenerator implements IntGenerator {

		public static final String ALPHA_PARAMETER_KEY = "gen.input.format.alpha";
		public static final String X_PARAMETER_KEY = "gen.input.format.x";
		
		private double alphaRev;
		private double x;
		
		@Override
		public void initialize(Configuration parameters) { 		
			
			this.alphaRev = (1.0 / parameters.getFloat(ALPHA_PARAMETER_KEY, 1.0f));
			this.x  = parameters.getFloat(X_PARAMETER_KEY, 1.0f);
		
			
		}

		@Override
		public int nextInt() {
			return (int)(x / Math.pow(Math.random(), alphaRev));
		}
		
	}
	
}