package eu.stratosphere.hadoopcompatibility.datatypes;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;


/**
 * Converter for the default hadoop writables.
 * Key will be in field 0, Value in field 1 of a Stratosphere Record.
 *
 */
public class DefaultHadoopTypeConverter<K, V> implements HadoopTypeConverter<K, V> {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record stratosphereRecord, K hadoopKey, V hadoopValue) {
		stratosphereRecord.setField(0, convert(hadoopKey)); 
		stratosphereRecord.setField(1, convert(hadoopValue)); 
	}
	
	private Value convert(Object hadoopType) {
		if(hadoopType instanceof org.apache.hadoop.io.LongWritable ) {
			return new LongValue(((LongWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.Text) {
			return new StringValue(((Text)hadoopType).toString());
		}
		if(hadoopType instanceof org.apache.hadoop.io.IntWritable) {
			return new IntValue(((IntWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.FloatWritable) {
			return new FloatValue(((FloatWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.DoubleWritable) {
			return new DoubleValue(((DoubleWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.BooleanWritable) {
			return new BooleanValue(((BooleanWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.ByteWritable) {
			return new ByteValue(((ByteWritable)hadoopType).get());
		}
		
		throw new RuntimeException("Unable to convert Hadoop type ("+hadoopType.getClass().getCanonicalName()+") to Stratosphere.");
	}
}
