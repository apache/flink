package eu.stratosphere.api.java.record.io.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.record.io.FileInputFormat;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;


/**
 * Input format to read Avro files.
 * 
 * The input format currently supports only flat avro schemas. So
 * there is no support for complex types except for nullable
 * primitve fields, e.g. ["string", null]
 * (See http://avro.apache.org/docs/current/spec.html#schema_complex)
 *
 */
public class AvroInputFormat extends FileInputFormat {
	private static final Log LOG = LogFactory.getLog(AvroInputFormat.class);
	
	
	private FileReader<GenericRecord> dataFileReader;
	private GenericRecord reuseAvroRecord = null;
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		SeekableInput in = new FSDataInputStreamWrapper(stream, (int)split.getLength());
		LOG.info("Opening split "+split);
		dataFileReader = DataFileReader.openReader(in, datumReader);
		dataFileReader.sync(split.getStart());
	}
	@Override
	public boolean reachedEnd() throws IOException {
		return !dataFileReader.hasNext();
	}

	@Override
	public boolean nextRecord(Record record) throws IOException {
		if(!dataFileReader.hasNext()) {
			return false;
		}
		if(record == null){
			throw new IllegalArgumentException("Empty PactRecord given");
		}
		reuseAvroRecord = dataFileReader.next(reuseAvroRecord);
		final List<Field> fields = reuseAvroRecord.getSchema().getFields();
		for(Field field : fields) {
			final Value value = convertAvroToPactValue(field, reuseAvroRecord.get(field.pos()));
			record.setField(field.pos(), value);
			record.updateBinaryRepresenation();
		}
		
		return true;
	}
	/**
	 * Converts an Avro GenericRecord to a Value.
	 * @return
	 */
	private StringValue sString = new StringValue();
	private IntValue sInt = new IntValue();
	private BooleanValue sBool = new BooleanValue();
	private DoubleValue sDouble = new DoubleValue();
	private FloatValue sFloat = new FloatValue();
	private LongValue sLong = new LongValue();
	
	private final Value convertAvroToPactValue(final Field field,final Object avroRecord) {
		if(avroRecord == null) {
			return null;
		}
		final Type type = checkTypeConstraintsAndGetType(field.schema());
		switch(type) {
			case STRING:
				sString.setValue((CharSequence) avroRecord);
				return sString;
			case INT:
				sInt.setValue((Integer) avroRecord);
				return sInt;
			case BOOLEAN:
				sBool.setValue((Boolean) avroRecord);
				return sBool;
			case DOUBLE:
				sDouble.setValue((Double) avroRecord);
				return sDouble;
			case FLOAT:
				sFloat.setValue((Float) avroRecord);
				return sFloat;
			case LONG:
				sLong.setValue((Long) avroRecord);
				return sLong;
			case NULL:
				return NullValue.getInstance();
			default:
				throw new RuntimeException("Type "+type+" for AvroInputFormat is not implemented. Open an issue on GitHub.");
		}
	}
	
	private final Type checkTypeConstraintsAndGetType(final Schema schema) {
		final Type type = schema.getType();
		if(type == Type.ARRAY || type == Type.ENUM || type == Type.RECORD || type == Type.MAP ) {
			throw new RuntimeException("The given Avro file contains complex data types which are not supported right now");
		}
		
		if( type == Type.UNION) {
			List<Schema> types = schema.getTypes();
			if(types.size() > 2) {
				throw new RuntimeException("The given Avro file contains a union that has more than two elements");
			}
			if(types.size() == 1 && types.get(0).getType() != Type.UNION) {
				return types.get(0).getType();
			}
			if(types.get(0).getType() == Type.UNION || types.get(1).getType() == Type.UNION ) {
				throw new RuntimeException("The given Avro file contains a nested union");
			}
			if(types.get(0).getType() == Type.NULL) {
				return types.get(1).getType();
			} else {
				if(types.get(1).getType() != Type.NULL) {
					throw new RuntimeException("The given Avro file is contains a union with two non-null types.");
				}
				return types.get(0).getType();
			}
		}
		return type;
	}

	/**
	 * Set minNumSplits to number of files.
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		int numAvroFiles = 0;
		final Path path = this.filePath;
		// get all the files that are involved in the splits
		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if(!acceptFile(pathFile)) {
			throw new IOException("The given file does not pass the file-filter");
		}
		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] dir = fs.listStatus(path);
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir() && acceptFile(dir[i])) {
					numAvroFiles++;
				}
			}
		} else {
			numAvroFiles = 1;
		}
		return super.createInputSplits(numAvroFiles);
	}
	
	// dirty hack. needs a fix!	
	private boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".");
	}
				
}
