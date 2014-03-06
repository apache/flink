/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.api.java.record.io.avro;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

import eu.stratosphere.api.avro.FSDataInputStreamWrapper;
import eu.stratosphere.api.java.record.io.FileInputFormat;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.ListValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.MapValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

/**
 * Input format to read Avro files.
 * 
 * The input format currently supports only flat avro schemas. So there is no
 * support for complex types except for nullable primitve fields, e.g.
 * ["string", null] (See
 * http://avro.apache.org/docs/current/spec.html#schema_complex)
 * 
 */
public class AvroRecordInputFormat extends FileInputFormat {
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(AvroRecordInputFormat.class);

	private FileReader<GenericRecord> dataFileReader;
	private GenericRecord reuseAvroRecord = null;

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		SeekableInput in = new FSDataInputStreamWrapper(stream, (int) split.getLength());
		LOG.info("Opening split " + split);
		dataFileReader = DataFileReader.openReader(in, datumReader);
		dataFileReader.sync(split.getStart());
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !dataFileReader.hasNext();
	}

	@Override
	public Record nextRecord(Record record) throws IOException {
		if (!dataFileReader.hasNext()) {
			return null;
		}
		if (record == null) {
			throw new IllegalArgumentException("Empty PactRecord given");
		}
		reuseAvroRecord = dataFileReader.next(reuseAvroRecord);
		final List<Field> fields = reuseAvroRecord.getSchema().getFields();
		for (Field field : fields) {
			final Value value = convertAvroToPactValue(field, reuseAvroRecord.get(field.pos()));
			record.setField(field.pos(), value);
			record.updateBinaryRepresenation();
		}

		return record;
	}


	@SuppressWarnings("unchecked")
	private final Value convertAvroToPactValue(final Field field, final Object avroRecord) {
		if (avroRecord == null) {
			return null;
		}
		final Type type = checkTypeConstraintsAndGetType(field.schema());

		// check for complex types
		// (complex type FIXED is not yet supported)
		switch (type) {
			case ARRAY:
				final Type elementType = field.schema().getElementType().getType();
				final List<?> avroList = (List<?>) avroRecord;
				return convertAvroArrayToListValue(elementType, avroList);
			case ENUM:
				final List<String> symbols = field.schema().getEnumSymbols();
				final String avroRecordString = avroRecord.toString();
				if (!symbols.contains(avroRecordString)) {
					throw new RuntimeException("The given Avro file contains field with a invalid enum symbol");
				}
				sString.setValue(avroRecordString);
				return sString;
			case MAP:
				final Type valueType = field.schema().getValueType().getType();
				final Map<CharSequence, ?> avroMap = (Map<CharSequence, ?>) avroRecord;
				return convertAvroMapToMapValue(valueType, avroMap);
	
			// primitive type
			default:
				return convertAvroPrimitiveToValue(type, avroRecord);

		}
	}

	private final ListValue<?> convertAvroArrayToListValue(Type elementType, List<?> avroList) {
		switch (elementType) {
		case STRING:
			StringListValue sl = new StringListValue();
			for (Object item : avroList) {
				sl.add(new StringValue((CharSequence) item));
			}
			return sl;
		case INT:
			IntListValue il = new IntListValue();
			for (Object item : avroList) {
				il.add(new IntValue((Integer) item));
			}
			return il;
		case BOOLEAN:
			BooleanListValue bl = new BooleanListValue();
			for (Object item : avroList) {
				bl.add(new BooleanValue((Boolean) item));
			}
			return bl;
		case DOUBLE:
			DoubleListValue dl = new DoubleListValue();
			for (Object item : avroList) {
				dl.add(new DoubleValue((Double) item));
			}
			return dl;
		case FLOAT:
			FloatListValue fl = new FloatListValue();
			for (Object item : avroList) {
				fl.add(new FloatValue((Float) item));
			}
			return fl;
		case LONG:
			LongListValue ll = new LongListValue();
			for (Object item : avroList) {
				ll.add(new LongValue((Long) item));
			}
			return ll;
		default:
			throw new RuntimeException("Elements of type " + elementType + " are not supported for Avro arrays.");
		}
	}

	private final MapValue<StringValue, ?> convertAvroMapToMapValue(Type mapValueType, Map<CharSequence, ?> avroMap) {
		switch (mapValueType) {
		case STRING:
			StringMapValue sm = new StringMapValue();
			for (Map.Entry<CharSequence, ?> entry : avroMap.entrySet()) {
				sm.put(new StringValue((CharSequence) entry.getKey()), new StringValue((String) entry.getValue()));
			}
			return sm;
		case INT:
			IntMapValue im = new IntMapValue();
			for (Map.Entry<CharSequence, ?> entry : avroMap.entrySet()) {
				im.put(new StringValue((CharSequence) entry.getKey()), new IntValue((Integer) entry.getValue()));
			}
			return im;
		case BOOLEAN:
			BooleanMapValue bm = new BooleanMapValue();
			for (Map.Entry<CharSequence, ?> entry : avroMap.entrySet()) {
				bm.put(new StringValue((CharSequence) entry.getKey()), new BooleanValue((Boolean) entry.getValue()));
			}
			return bm;
		case DOUBLE:
			DoubleMapValue dm = new DoubleMapValue();
			for (Map.Entry<CharSequence, ?> entry : avroMap.entrySet()) {
				dm.put(new StringValue((CharSequence) entry.getKey()), new DoubleValue((Double) entry.getValue()));
			}
			return dm;
		case FLOAT:
			FloatMapValue fm = new FloatMapValue();
			for (Map.Entry<CharSequence, ?> entry : avroMap.entrySet()) {
				fm.put(new StringValue((CharSequence) entry.getKey()), new FloatValue((Float) entry.getValue()));
			}
			return fm;
		case LONG:
			LongMapValue lm = new LongMapValue();
			for (Map.Entry<CharSequence, ?> entry : avroMap.entrySet()) {
				lm.put(new StringValue((CharSequence) entry.getKey()), new LongValue((Long) entry.getValue()));
			}
			return lm;

		default:
			throw new RuntimeException("Map values of type " + mapValueType + " are not supported for Avro map.");
		}
	}

	private StringValue sString = new StringValue();
	private IntValue sInt = new IntValue();
	private BooleanValue sBool = new BooleanValue();
	private DoubleValue sDouble = new DoubleValue();
	private FloatValue sFloat = new FloatValue();
	private LongValue sLong = new LongValue();
	
	private final Value convertAvroPrimitiveToValue(Type type, Object avroRecord) {
		switch (type) {
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
			throw new RuntimeException(
					"Type "
							+ type
							+ " for AvroInputFormat is not implemented. Open an issue on GitHub.");
		}
	}

	private final Type checkTypeConstraintsAndGetType(final Schema schema) {
		final Type type = schema.getType();
		if (type == Type.RECORD) {
			throw new RuntimeException("The given Avro file contains complex data types which are not supported right now");
		}

		if (type == Type.UNION) {
			List<Schema> types = schema.getTypes();
			if (types.size() > 2) {
				throw new RuntimeException("The given Avro file contains a union that has more than two elements");
			}
			if (types.size() == 1 && types.get(0).getType() != Type.UNION) {
				return types.get(0).getType();
			}
			if (types.get(0).getType() == Type.UNION || types.get(1).getType() == Type.UNION) {
				throw new RuntimeException("The given Avro file contains a nested union");
			}
			if (types.get(0).getType() == Type.NULL) {
				return types.get(1).getType();
			} else {
				if (types.get(1).getType() != Type.NULL) {
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
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		int numAvroFiles = 0;
		final Path path = this.filePath;
		// get all the files that are involved in the splits
		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (!acceptFile(pathFile)) {
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

	// --------------------------------------------------------------------------------------------
	// Concrete subclasses of ListValue and MapValue for all possible primitive types
	// --------------------------------------------------------------------------------------------

	public static class StringListValue extends ListValue<StringValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class IntListValue extends ListValue<IntValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class BooleanListValue extends ListValue<BooleanValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class DoubleListValue extends ListValue<DoubleValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class FloatListValue extends ListValue<FloatValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class LongListValue extends ListValue<LongValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class StringMapValue extends MapValue<StringValue, StringValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class IntMapValue extends MapValue<StringValue, IntValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class BooleanMapValue extends MapValue<StringValue, BooleanValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class DoubleMapValue extends MapValue<StringValue, DoubleValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class FloatMapValue extends MapValue<StringValue, FloatValue> {
		private static final long serialVersionUID = 1L;
	}

	public static class LongMapValue extends MapValue<StringValue, LongValue> {
		private static final long serialVersionUID = 1L;
	}

}
