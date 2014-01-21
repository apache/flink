package eu.stratosphere.hadoopcompat.datatypes;

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
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;


public class HadoopTypeConverter<ST extends Value,HT>{

	public ST convert(HT hadoopType)
	{
		if(hadoopType==null)
			return null;
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.LongWritable")==0)
		{
			return (ST) new LongValue(((LongWritable)hadoopType).get());
		}
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.Text")==0)
		{
			return (ST) new StringValue(((Text)hadoopType).toString());
		}
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.IntWritable")==0)
		{
			return (ST) new IntValue(((IntWritable)hadoopType).get());
		}
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.FloatWritable")==0)
		{
			return (ST) new FloatValue(((FloatWritable)hadoopType).get());
		}
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.DoubleWritable")==0)
		{
			return (ST) new DoubleValue(((DoubleWritable)hadoopType).get());
		}
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.BooleanWritable")==0)
		{
			return (ST) new BooleanValue(((BooleanWritable)hadoopType).get());
		}
		if(hadoopType.getClass().getName().compareTo("org.apache.hadoop.io.ByteWritable")==0)
		{
			return (ST) new ByteValue(((ByteWritable)hadoopType).get());
		}
		return null;
	}

}
