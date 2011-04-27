package eu.stratosphere.pact.runtime.histogram;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class AdaptiveSampleStub extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {
	private Random 	rnd;
	
	private int[] 	positions;
	private int[] 	lengths;
	private double[] 	rands;
	private	boolean[] deleted;
	
	private long 	count;
	private	int		sampleSize;
	private int		capacity;
	private double	sqrt;
	private double	invSqrt;
	private int		nextSqrt;
	
	private DataOutput dataOutput;
	private BufferedOutputStream outBuffer;
	
	private Collector<PactInteger, PactInteger> out;
	
	
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		count = 0;
		sampleSize = 0;
		capacity = 1024;
		
		positions = new int[capacity];
		lengths = new int[capacity];
		rands = new double[capacity];
		deleted = new boolean[capacity];
		
		sqrt = 1;
		invSqrt = 1/sqrt;
		nextSqrt = 2;
		
		/*
		 * Manage buffer and output for serialization of data
		 */
		outBuffer = new BufferedOutputStream();
		//TODO: Use estimations from compiler on tuple size instead of 1024 bytes
		outBuffer.data = new byte[capacity * 1];
		outBuffer.basePos = 0;
		
		dataOutput = new DataOutputStream(outBuffer);
	}

	@Override
	public void open() {
		super.open();
		rnd = new Random(1988);
	}
	
	@Override
	public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
		this.out = out;
		
		/*
		 * The selectivity is based on the number of elements in the data set. Because this is
		 * not know beforehand it has to be adjusted during runtime. A representative sample
		 * of a data set should have the cardinality sqrt(n) => sqrt(count) in this code. 
		 */
		if(count == nextSqrt) {
			updateSqrt();
		}
		
		
		double rand = rnd.nextDouble();
		boolean useAsSample = rand < invSqrt;
		
		if(useAsSample) {
			//Adjust capacity if arrays too small
			if(sampleSize+1 == capacity) {
				adjustArrays();
			}
			
			//Write data
			outBuffer.basePos = positions[sampleSize];
			outBuffer.offset = 0;
			
			try {
				key.write(dataOutput);
			} catch (IOException e) {
				//TODO: Proper error handling
				throw new RuntimeException(e);
			}
			
			//Calculate length and data position of next entry
			//Attention: sampleSize could have been changed during writing because of that
			//the position cannot be calculated with positions[sampleSize]
			rands[sampleSize] = rand;
			lengths[sampleSize] = outBuffer.offset;
			positions[sampleSize] = outBuffer.basePos;
			//Might still be true from a value that was removed during compaction
			deleted[sampleSize] = false; 
			positions[sampleSize+1] = positions[sampleSize] + lengths[sampleSize];
			
			sampleSize++;
		}
		
		count++;
	}
	
	private void refilterSample() {
		updateSqrt();
		
		//Filters the sample and throws all values out that would not be considered anymore
		//using the current sqrt. Old entries are not immediately removed from the array
		//but only when buffer is full, instead they are marked invalid by setting deleted
		//to true
		
		for (int i = 0; i < sampleSize; i++) {
			if(!deleted[i]) {
				boolean useAsSample = rands[i] < invSqrt;
				if(!useAsSample) {
					deleted[i] = true;
				}
			}
		}
	}
	
	private void updateSqrt() {
		sqrt = Math.sqrt(count);
		invSqrt = 1/sqrt;
		nextSqrt = (int) (1.21 * count);
		//For small counts (e.g. 4) this case happens.
		if(nextSqrt <= count) {
			nextSqrt = (int) (count*2);
		}
		//Then the sqrt is 10%x bigger => more slective, if multiplicator
		//is higher than less sqrt will be calculated, but it is more
		//probable that samples need to be filtered out afterwards and thus
		//are unnecessarily serialized
	}
	
	private void adjustArrays() {
		capacity = capacity * 2;
		
		int[] newPositions = new int[capacity];
		int[] newLengths = new int[capacity];
		double[] newRands = new double[capacity];
		boolean[] newDeleted = new boolean[capacity];
		
		System.arraycopy(positions, 0, newPositions, 0, sampleSize);
		System.arraycopy(lengths, 0, newLengths, 0, sampleSize);
		System.arraycopy(rands, 0, newRands, 0, sampleSize);
		System.arraycopy(deleted, 0, newDeleted, 0, sampleSize);
		
		positions = newPositions;
		lengths = newLengths;
		rands = newRands;
		deleted = newDeleted;
	}
	
	@Override
	public void close() {
		refilterSample();
		
		PactInteger k = null;		
		BufferedInputStream inBuffer = new BufferedInputStream();
		inBuffer.data = outBuffer.data;
		DataInput dataInput = new DataInputStream(inBuffer);
		
		for (int i = 0; i < sampleSize; i++) {
			if(!deleted[i]) {
				inBuffer.basePos = positions[i];
				inBuffer.length = lengths[i];
				inBuffer.offset = 0;
				
				try {
					k = this.getOutKeyType().newInstance();
					k.read(dataInput);
					out.collect(k, k);
				} catch (Exception e) {
					//TODO: proper error handling
					throw new RuntimeException(e);
				}
			}
		}
	}

	private class BufferedOutputStream extends OutputStream {
		byte[] data;
		int basePos;
		int offset;
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public void flush() throws IOException {
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if(len > data.length - basePos - offset) {
				adjustBuffer(len);
			}
			
			System.arraycopy(b, off, data, basePos + offset, len);
			offset += len;
		}

		@Override
		public void write(byte[] b) throws IOException {
			if(b.length > data.length - basePos - offset) {
				adjustBuffer(b.length);
			}
			
			System.arraycopy(b, 0, data, basePos + offset, b.length);
			offset += b.length;
		}

		@Override
		public void write(int b) throws IOException {
			if(data.length == basePos + offset) {
				adjustBuffer(1);
			}
			
			data[basePos + offset] = (byte) (b & 0xFF);
			offset += 1;
		}
		
		private void adjustBuffer(int required) {
			refilterSample();
			
			/*
			 * Decide whether it is necessary to resize buffer or whether it is
			 * enough to compact it. If the size of the valid data + the additional 
			 * required data is smaller than half of the buffer it is only compacted
			 * and not resized.
			 */
			int validSize = 0;
			for (int i = 0; i < sampleSize; i++) {
				if(!deleted[i]) {
					validSize += lengths[i]; 
				}
			}
			
			int totalSize = validSize + required;
			boolean compactOnly = data.length / totalSize > 2;
			
			byte[] target = null;
			if(compactOnly) {
				target = data;
			} else {
				//TODO: Could create endless loop here if data is too large
				// and integer overflows?
				//Double size until all data fits in
				int newSize = data.length * 2;
				while((newSize = newSize * 2) < totalSize);
				
				target = new byte[newSize];
			}
			
			//Compact (& move) data
			int newCount = 0;
			int currPos = 0;
			for (int i = 0; i < sampleSize; i++) {
				if(!deleted[i]) {
					System.arraycopy(data, positions[i], target, currPos, lengths[i]);
					deleted[newCount] = false;
					positions[newCount] = positions[i];
					lengths[newCount] = lengths[i];
					rands[newCount] = rands[i];
					
					currPos += lengths[newCount];
					newCount++;
				}
			}
			
			//Move data for the current written record
			System.arraycopy(data, positions[newCount], target, currPos, offset);
			positions[newCount] = currPos;
			
			//Adjust stream base position;
			basePos = positions[newCount];
			
			/*
			 * Use target as new buffer, notice that in the case that we did
			 * only compact: data == target.
			 */
			sampleSize = newCount;
			data = target;
		}
	}
	
	private class BufferedInputStream extends InputStream {
		private byte[] data;
		private int basePos;
		private int offset;
		private int length;
		
		@Override
		public int available() throws IOException {
			return length - offset;
		}

		@Override
		public boolean markSupported() {
			return false;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			//Check end of stream
			if(offset == length) {
				return -1;
			}
			
			int readLen = Math.min(len, length-offset);
			System.arraycopy(data, basePos+offset, b, off, readLen);
			
			offset += readLen;
			
			return readLen;
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
		
		@Override
		public int read() throws IOException {
			//Check end of stream
			if(offset == length) {
				return -1;
			}
			
			int res = data[basePos + offset] & 0xFF;
			offset++;
		
			return res;
		}

		@Override
		public long skip(long len) throws IOException {
			return offset += len;
		}
	}
}
