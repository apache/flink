package org.apache.flink.api.java.io;

import java.beans.Transient;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * This class represent a row as a POJO
 * 
 * */
public class GenericRow implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	private Object[] fields;
	
	/** @deprecated just to implement POJO */
	public GenericRow() {
	}
	
	public GenericRow(int size) {
		this.fields = new Object[size];
	}
	
	@Transient
	public int getArity() {
		return fields.length;
	}
	
	/**
	 * Gets the field at the specified position.
	 *
	 * @param pos The position of the field, zero indexed.
	 * @return The field at the specified position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 */
	@Transient
	public Object getField(int pos) {
		return fields[pos];
	}
	
	/**
	 * Sets the field at the specified position.
	 *
	 * @param value The value to be assigned to the field at the specified position.
	 * @param pos The position of the field, zero indexed.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 */
	@Transient
	public void setField(Object value, int pos) {
		fields[pos] = value;
	}

	public Object[] getFields() {
		return fields;
	}

	public void setFields(Object[] fields) {
		this.fields = fields;
	}

	/**
	 * Transform a SqlRow into a Tuple object.
	 * 
	 * @throws InstantiationException,IllegalAccessException if not compatible with Tuple constraints (e.g. MAX 25 fields)
	 * */
	@Transient
	public Tuple toTuple() throws InstantiationException, IllegalAccessException {
		Tuple ret = Tuple.getTupleClass(getArity()).newInstance();
		for (int pos = 0; pos < fields.length; pos++) {
			Object value = fields[pos];
			ret.setField(value, pos);
		}
		return ret;
	}
	
	/**
	 * Transform a Tuple into a SqlRow object.
	 * 
	 * */
	@Transient
	public GenericRow fromTuple(Tuple tuple) {
		if(this.fields == null || this.fields.length == 0) {
			this.fields = new Object[tuple.getArity()];
		}
		for (int i = 0; i < fields.length; i++) {
			fields[i] = tuple.getField(i);
		}
		return this;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(fields);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		GenericRow other = (GenericRow) obj;
		if (!Arrays.equals(fields, other.fields)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "SqlRow [" + Arrays.toString(fields) + "]";
	}
	
}
