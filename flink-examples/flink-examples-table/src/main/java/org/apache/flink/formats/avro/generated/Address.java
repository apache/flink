/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.generated;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Address extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -1371728748107250838L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"org.apache.flink.formats.avro.generated\",\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Address> ENCODER =
      new BinaryMessageEncoder<Address>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Address> DECODER =
      new BinaryMessageDecoder<Address>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<Address> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<Address> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Address>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this Address to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a Address from a ByteBuffer. */
  public static Address fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public int num;
  @Deprecated public CharSequence street;
  @Deprecated public CharSequence city;
  @Deprecated public CharSequence state;
  @Deprecated public CharSequence zip;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Address() {}

  /**
   * All-args constructor.
   * @param num The new value for num
   * @param street The new value for street
   * @param city The new value for city
   * @param state The new value for state
   * @param zip The new value for zip
   */
  public Address(Integer num, CharSequence street, CharSequence city, CharSequence state, CharSequence zip) {
    this.num = num;
    this.street = street;
    this.city = city;
    this.state = state;
    this.zip = zip;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return num;
    case 1: return street;
    case 2: return city;
    case 3: return state;
    case 4: return zip;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: num = (Integer)value$; break;
    case 1: street = (CharSequence)value$; break;
    case 2: city = (CharSequence)value$; break;
    case 3: state = (CharSequence)value$; break;
    case 4: zip = (CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'num' field.
   * @return The value of the 'num' field.
   */
  public Integer getNum() {
    return num;
  }

  /**
   * Sets the value of the 'num' field.
   * @param value the value to set.
   */
  public void setNum(Integer value) {
    this.num = value;
  }

  /**
   * Gets the value of the 'street' field.
   * @return The value of the 'street' field.
   */
  public CharSequence getStreet() {
    return street;
  }

  /**
   * Sets the value of the 'street' field.
   * @param value the value to set.
   */
  public void setStreet(CharSequence value) {
    this.street = value;
  }

  /**
   * Gets the value of the 'city' field.
   * @return The value of the 'city' field.
   */
  public CharSequence getCity() {
    return city;
  }

  /**
   * Sets the value of the 'city' field.
   * @param value the value to set.
   */
  public void setCity(CharSequence value) {
    this.city = value;
  }

  /**
   * Gets the value of the 'state' field.
   * @return The value of the 'state' field.
   */
  public CharSequence getState() {
    return state;
  }

  /**
   * Sets the value of the 'state' field.
   * @param value the value to set.
   */
  public void setState(CharSequence value) {
    this.state = value;
  }

  /**
   * Gets the value of the 'zip' field.
   * @return The value of the 'zip' field.
   */
  public CharSequence getZip() {
    return zip;
  }

  /**
   * Sets the value of the 'zip' field.
   * @param value the value to set.
   */
  public void setZip(CharSequence value) {
    this.zip = value;
  }

  /**
   * Creates a new Address RecordBuilder.
   * @return A new Address RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new Address RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Address RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }

  /**
   * Creates a new Address RecordBuilder by copying an existing Address instance.
   * @param other The existing instance to copy.
   * @return A new Address RecordBuilder
   */
  public static Builder newBuilder(Address other) {
    return new Builder(other);
  }

  /**
   * RecordBuilder for Address instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Address>
    implements org.apache.avro.data.RecordBuilder<Address> {

    private int num;
    private CharSequence street;
    private CharSequence city;
    private CharSequence state;
    private CharSequence zip;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.num)) {
        this.num = data().deepCopy(fields()[0].schema(), other.num);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.street)) {
        this.street = data().deepCopy(fields()[1].schema(), other.street);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.city)) {
        this.city = data().deepCopy(fields()[2].schema(), other.city);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.state)) {
        this.state = data().deepCopy(fields()[3].schema(), other.state);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.zip)) {
        this.zip = data().deepCopy(fields()[4].schema(), other.zip);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing Address instance
     * @param other The existing instance to copy.
     */
    private Builder(Address other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.num)) {
        this.num = data().deepCopy(fields()[0].schema(), other.num);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.street)) {
        this.street = data().deepCopy(fields()[1].schema(), other.street);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.city)) {
        this.city = data().deepCopy(fields()[2].schema(), other.city);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.state)) {
        this.state = data().deepCopy(fields()[3].schema(), other.state);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.zip)) {
        this.zip = data().deepCopy(fields()[4].schema(), other.zip);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'num' field.
      * @return The value.
      */
    public Integer getNum() {
      return num;
    }

    /**
      * Sets the value of the 'num' field.
      * @param value The value of 'num'.
      * @return This builder.
      */
    public Builder setNum(int value) {
      validate(fields()[0], value);
      this.num = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'num' field has been set.
      * @return True if the 'num' field has been set, false otherwise.
      */
    public boolean hasNum() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'num' field.
      * @return This builder.
      */
    public Builder clearNum() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'street' field.
      * @return The value.
      */
    public CharSequence getStreet() {
      return street;
    }

    /**
      * Sets the value of the 'street' field.
      * @param value The value of 'street'.
      * @return This builder.
      */
    public Builder setStreet(CharSequence value) {
      validate(fields()[1], value);
      this.street = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'street' field has been set.
      * @return True if the 'street' field has been set, false otherwise.
      */
    public boolean hasStreet() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'street' field.
      * @return This builder.
      */
    public Builder clearStreet() {
      street = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'city' field.
      * @return The value.
      */
    public CharSequence getCity() {
      return city;
    }

    /**
      * Sets the value of the 'city' field.
      * @param value The value of 'city'.
      * @return This builder.
      */
    public Builder setCity(CharSequence value) {
      validate(fields()[2], value);
      this.city = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'city' field has been set.
      * @return True if the 'city' field has been set, false otherwise.
      */
    public boolean hasCity() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'city' field.
      * @return This builder.
      */
    public Builder clearCity() {
      city = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'state' field.
      * @return The value.
      */
    public CharSequence getState() {
      return state;
    }

    /**
      * Sets the value of the 'state' field.
      * @param value The value of 'state'.
      * @return This builder.
      */
    public Builder setState(CharSequence value) {
      validate(fields()[3], value);
      this.state = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'state' field has been set.
      * @return True if the 'state' field has been set, false otherwise.
      */
    public boolean hasState() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'state' field.
      * @return This builder.
      */
    public Builder clearState() {
      state = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'zip' field.
      * @return The value.
      */
    public CharSequence getZip() {
      return zip;
    }

    /**
      * Sets the value of the 'zip' field.
      * @param value The value of 'zip'.
      * @return This builder.
      */
    public Builder setZip(CharSequence value) {
      validate(fields()[4], value);
      this.zip = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'zip' field has been set.
      * @return True if the 'zip' field has been set, false otherwise.
      */
    public boolean hasZip() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'zip' field.
      * @return This builder.
      */
    public Builder clearZip() {
      zip = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Address build() {
      try {
        Address record = new Address();
        record.num = fieldSetFlags()[0] ? this.num : (Integer) defaultValue(fields()[0]);
        record.street = fieldSetFlags()[1] ? this.street : (CharSequence) defaultValue(fields()[1]);
        record.city = fieldSetFlags()[2] ? this.city : (CharSequence) defaultValue(fields()[2]);
        record.state = fieldSetFlags()[3] ? this.state : (CharSequence) defaultValue(fields()[3]);
        record.zip = fieldSetFlags()[4] ? this.zip : (CharSequence) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Address>
    WRITER$ = (org.apache.avro.io.DatumWriter<Address>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Address>
    READER$ = (org.apache.avro.io.DatumReader<Address>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
