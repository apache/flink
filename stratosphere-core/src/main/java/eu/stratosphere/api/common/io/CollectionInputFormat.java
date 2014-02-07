package eu.stratosphere.api.common.io;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.ValueUtil;

/**
 * input format for java collection input. It can accept collection data or serializable iterator
 *
 */
public class CollectionInputFormat extends GenericInputFormat<Record> implements UnsplittableInput {

	private static final long serialVersionUID = 1L;

	private Collection<?> dataSet;		//input data as collection

	private Iterator<?> serializableIter;	//input data as serializable iterator

	private transient Iterator<?> it;

	@Override
	public boolean reachedEnd() throws IOException {
		return !it.hasNext();
	}


	@Override
	public void open(GenericInputSplit split) throws IOException {
        super.open(split);
		if (serializableIter != null) {
			it = serializableIter;
        }
		else {
			it = this.dataSet.iterator();
        }
	}

	@Override
	public boolean nextRecord(Record record) throws IOException {
		if (it.hasNext()) {
            record.clear();
            Object b = it.next();
            //check whether the record field is one-dimensional or multi-dimensional
            if (b.getClass().isArray()) {
                for (Object s : (Object[])b){
                    record.addField(ValueUtil.toStratosphere(s));
                }
            }
            else if (b instanceof Collection) {
                @SuppressWarnings("unchecked")
                Iterator<Object> tmpIter = ((Collection<Object>) b).iterator();
                while (tmpIter.hasNext()) {
                    Object s = tmpIter.next();
                    record.addField(ValueUtil.toStratosphere(s));
                }
            }
            else {
                record.setField(0, ValueUtil.toStratosphere(b));
            }
            return true;
        } else {
			return false;
		}
	}

	public void setData(Collection<?> data) {
		this.dataSet = data;
		this.serializableIter = null;
	}

	public<T extends Iterator<?>,Serializable> void setIter(T iter)  {
		this.serializableIter = iter;
	}
}
