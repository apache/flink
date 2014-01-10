package eu.stratosphere.api.common.io;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.api.common.operators.util.SerializableIterator;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.ValueUtil;

/**
 * input format for java collection input. It can accept collection data or serializable iterator
 * 
 */
public class CollectionInputFormat extends GenericInputFormat<Record> implements UnsplittableInput {


	private static final long serialVersionUID = 1L;

	private transient boolean end;
	
	private Collection<Object> steam;		//input data as collection
	
	private SerializableIterator<Object> serializableIter;	//input data as serializable iterator
	
	private transient Iterator<Object> it;
	
	private transient Object currObject;
	
	@Override
	public boolean reachedEnd() throws IOException {
		return this.end;
	}

	/**
	 * get the next Object
	 */
	public boolean readObject() {
		if (it.hasNext()) {
			currObject = it.next();
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * decode the record from one Object. The record could have multiple fields.
	 */
	public boolean readRecord(Record target, Object b) {
		target.clear();
		//check whether the record field is one-dimensional or multi-dimensional
		if (b.getClass().isArray()) {
			for (Object s : (Object[])b){
				target.addField(ValueUtil.toStratosphere(s));
			}
		}
		else if (b instanceof Collection) {
			@SuppressWarnings("unchecked")
			Iterator<Object> tmp_it = ((Collection<Object>) b).iterator();
			while (tmp_it.hasNext())
			{
				Object s = tmp_it.next();
				target.addField(ValueUtil.toStratosphere(s));
			}
		}
		else {
			target.setField(0, ValueUtil.toStratosphere(b));
		}
		return true;	
	}
	
	@Override
	public void open(GenericInputSplit split) throws IOException {
		this.partitionNumber = split.getSplitNumber();
		if (serializableIter != null)
			it = serializableIter;
		else
			it = this.steam.iterator();
	}
	
	@Override
	public boolean nextRecord(Record record) throws IOException {
		if (readObject()) {
			return readRecord(record, this.currObject);
		} else {
			this.end = true;
			return false;
		}
	}
	
	public void setData(Collection<Object> data) {
		this.steam = data;
		this.serializableIter = null;
	}
	
	public void setIter(SerializableIterator<Object> iter) {
		this.serializableIter = iter;
	}

}
