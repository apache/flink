/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.Charset;

import static org.apache.flink.table.api.types.DecimalType.MAX_PRECISION;

/**
 * Utility class that handles ordered byte arrays. That is, these methods produce
 * byte arrays which maintain the sort order of the original values.
 *
 * <h3>String</h3>
 * <p>
 * There are zero or more intervening bytes that encode the text
 * value. The intervening bytes are chosen so that the encoding will sort in
 * the desired collating order.
 * </p>
 * <h3>ByteArray</h3>
 * <p>
 * "ByteArray" is a simple byte-for-byte copy of the input data.
 * </p>
 * <h3>Variable-length BigDecimal Encoding</h3>
 * <p>
 * For all numeric values, we compute a mantissa M and an exponent E. The
 * mantissa is a base-100 representation of the value. The exponent E
 * determines where to put the decimal point.
 * </p>
 * <p>
 * Each centimal digit of the mantissa is stored in a byte.
 * </p>
 * <p>
 * If we assume all digits of the mantissa occur to the right of the decimal
 * point, then the exponent E is the power of one hundred by which one must
 * multiply the mantissa to recover the original value.
 * </p>
 * <p>
 * Values are classified as large, medium, or small according to the value of
 * E. If E is 11 or more, the value is large. For E between 0 and 10, the
 * value is medium. For E less than zero, the value is small.
 * </p>
 * <p>
 * Large positive values are encoded as a single byte 0x22 followed by E as a
 * varint and then M. Medium positive values are a single byte of 0x17+E
 * followed by M. Small positive values are encoded as a single byte 0x16
 * followed by the ones-complement of the varint for -E followed by M.
 * </p>
 * <p>
 * Small negative values are encoded as a single byte 0x14 followed by -E as a
 * varint and then the ones-complement of M. Medium negative values are
 * encoded as a byte 0x13-E followed by the ones-complement of M. Large
 * negative values consist of the single byte 0x08 followed by the
 * ones-complement of the varint encoding of E followed by the ones-complement
 * of M.
 * </p>
 * <h3>Fixed-length Integer Encoding</h3>
 * <p>
 * All 4-byte integers are serialized to a 4-byte, fixed-width, sortable byte
 * format. All 8-byte integers are serialized to the equivelant 8-byte format.
 * Serialization is performed by inverting the integer
 * sign bit and writing the resulting bytes to the byte array in big endian
 * order.
 * </p>
 * <h3>Fixed-length Floating Point Encoding</h3>
 * <p>
 * 32-bit and 64-bit floating point numbers are encoded to a 4-byte and 8-byte
 * encoding format, respectively. The format is identical, save for the
 * precision respected in each step of the operation.
 * </p>
 * <p>
 * Floating point numbers are encoded as specified in IEEE 754. A 32-bit
 * single precision float consists of a sign bit, 8-bit unsigned exponent
 * encoded in offset-127 notation, and a 23-bit significand. The format is
 * described further in the <a
 * href="http://en.wikipedia.org/wiki/Single_precision"> Single Precision
 * Floating Point Wikipedia page</a>
 * </p>
 * <p>
 * The value of a normal float is -1 <sup>sign bit</sup> &times;
 * 2<sup>exponent - 127</sup> &times; 1.significand
 * </p>
 * <p>
 * The IEE754 floating point format already preserves sort ordering for
 * positive floating point numbers when the raw bytes are compared in most
 * significant byte order. This is discussed further at <a href=
 * "http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
 * http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm</a>
 * </p>
 * <p>
 * Thus, we need only ensure that negative numbers sort in the the exact
 * opposite order as positive numbers (so that say, negative infinity is less
 * than negative 1), and that all negative numbers compare less than any
 * positive number. To accomplish this, we invert the sign bit of all floating
 * point numbers, and we also invert the exponent and significand bits if the
 * floating point number was negative.
 * </p>
 * <p>
 * More specifically, we first store the floating point bits into a 32-bit int
 * {@code j} using {@link Float#floatToIntBits}. This method collapses
 * all NaNs into a single, canonical NaN value but otherwise leaves the bits
 * unchanged. We then compute
 * </p>
 *
 * <pre>
 * j &circ;= (j &gt;&gt; (Integer.SIZE - 1)) | Integer.MIN_SIZE
 * </pre>
 * <p>
 * which inverts the sign bit and XOR's all other bits with the sign bit
 * itself. Comparing the raw bytes of {@code j} in most significant byte
 * order is equivalent to performing a single precision floating point
 * comparison on the underlying bits (ignoring NaN comparisons, as NaNs don't
 * compare equal to anything when performing floating point comparisons).
 * </p>
 * <p>
 * The resulting integer is then converted into a byte array by serializing
 * the integer one byte at a time in most significant byte order.
 * </p>
 * <p>
 * {@code OrderedBytes} encodings are influenced by the HBase order encoding strategy
 * which is heavily influenced by the <a href="http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki">
 * SQLite4 Key Encoding</a>. Slight deviations are make in the interest of order
 * correctness and user extensibility. Fixed-width {@code Long} and
 * {@link Double} encodings are based on implementations from the now defunct
 * Orderly library.
 * </p>
 */
public final class OrderedBytes implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The following constant values are used by encoding implementations.
	 */
	public static final Charset UTF8 = Charset.forName("UTF-8");

	private static final byte TERM = 0x00;
	private static final byte NEG_LARGE = 0x08;
	private static final byte NEG_MED_MIN = 0x09;
	private static final byte NEG_MED_MAX = 0x13;
	private static final byte NEG_SMALL = 0x14;
	private static final byte ZERO = 0x15;
	private static final byte POS_SMALL = 0x16;
	private static final byte POS_MED_MIN = 0x17;
	private static final byte POS_MED_MAX = 0x21;
	private static final byte POS_LARGE = 0x22;

	private static final BigDecimal E8 = BigDecimal.valueOf(1e8);
	private static final BigDecimal E32 = BigDecimal.valueOf(1e32);
	private static final BigDecimal EN2 = BigDecimal.valueOf(1e-2);
	private static final BigDecimal EN10 = BigDecimal.valueOf(1e-10);

	/**
	 * The context used to normalize {@link BigDecimal} values.
	 */
	private static final MathContext DEFAULT_MATH_CONTEXT =
		new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);

	private static final int BUFFER_SIZE = 256;

	private transient byte[] reuseBuffer;

	private transient ByteArrayOutputStreamWithPos reuseBaos;

	/**
	 * Strip all trailing zeros to ensure that no digit will be zero and round
	 * using our default context to ensure precision doesn't exceed max allowed.
	 * From Phoenix's {@code NumberUtil}.
	 * @return new {@link BigDecimal} instance
	 */
	private static BigDecimal normalize(BigDecimal val) {
		return null == val ? null : val.stripTrailingZeros().round(DEFAULT_MATH_CONTEXT);
	}

	/**
	 * Encode a numerical value using the variable-length encoding.
	 * @param target The destination to which encoded digits are written.
	 * @param val The value to encode.
	 * @param ord The {@link Order} to respect while encoding {@code val}.
	 * @return the number of bytes written.
	 */
	public int encodeBigDecimal(DataOutputView target, BigDecimal val, Order ord) throws IOException {
		final int len;
		if (BigDecimal.ZERO.compareTo(val) == 0) {
			target.writeByte(ord.apply(ZERO));
			return 1;
		}
		BigDecimal abs = val.abs();
		if (BigDecimal.ONE.compareTo(abs) <= 0) { // abs(v) >= 1.0
			len = encodeBigDecimalLarge(target, normalize(val), ord);
		} else { // 1.0 > abs(v) >= 0.0
			len = encodeBigDecimalSmall(target, normalize(val), ord);
		}
		return len;
	}

	/**
	 * <p>
	 * Encode the small magnitude floating point number {@code val} using the
	 * key encoding. The caller guarantees that 1.0 > abs(val) > 0.0.
	 * </p>
	 * <p>
	 * A floating point value is encoded as an integer exponent {@code E} and a
	 * mantissa {@code M}. The original value is equal to {@code (M * 100^E)}.
	 * {@code E} is set to the smallest value possible without making {@code M}
	 * greater than or equal to 1.0.
	 * </p>
	 * <p>
	 * For this routine, {@code E} will always be zero or negative, since the
	 * original value is less than one. The encoding written by this routine is
	 * the ones-complement of the varint of the negative of {@code E} followed
	 * by the mantissa:
	 * <pre>
	 *   Encoding:   ~-E  M
	 * </pre>
	 * </p>
	 * @param target The destination to which encoded digits are written.
	 * @param val The value to encode.
	 * @return the number of bytes written.
	 */
	private int encodeBigDecimalSmall(DataOutputView target, BigDecimal val, Order ord) throws IOException {
		// assert 1.0 > abs(val) > 0.0
		BigDecimal abs = val.abs();
		assert BigDecimal.ZERO.compareTo(abs) < 0 && BigDecimal.ONE.compareTo(abs) > 0;
		boolean isNeg = val.signum() == -1;
		int e = 0;
		int length = 0;

		if (isNeg) { /* Small negative number: 0x14, -E, ~M */
			target.writeByte(ord.apply(NEG_SMALL));
		} else { /* Small positive number: 0x16, ~-E, M */
			target.writeByte(ord.apply(POS_SMALL));
		}
		length++;

		// normalize abs(val) to determine E
		while (abs.compareTo(EN10) < 0) {
			abs = abs.movePointRight(8);
			e += 4;
		}
		while (abs.compareTo(EN2) < 0) {
			abs = abs.movePointRight(2);
			e++;
		}

		// encode E value.
		// positive values encoded as ~E
		length += encodeInt(target, e, getCompositeOrder(!isNeg ? Order.DESCENDING : Order.ASCENDING, ord));

		Order compOrd = getCompositeOrder(isNeg ? Order.DESCENDING : Order.ASCENDING, ord);
		// encode M by peeling off centimal digits
		// TODO: 18 is an arbitrary encoding limit. Reevaluate once we have a better handling of
		// numeric scale.
		for (int i = 0; i < 18 && abs.compareTo(BigDecimal.ZERO) != 0; i++) {
			abs = abs.movePointRight(2);
			int d = abs.intValue();
			target.writeByte(compOrd.apply((byte) (d & 0xff)));
			abs = abs.subtract(BigDecimal.valueOf(d));
			length++;
		}
		return length;
	}

	/**
	 * Encode the large magnitude floating point number {@code val} using
	 * the key encoding. The caller guarantees that {@code val} will be
	 * finite and abs(val) >= 1.0.
	 * <p>
	 * A floating point value is encoded as an integer exponent {@code E}
	 * and a mantissa {@code M}. The original value is equal to
	 * {@code (M * 100^E)}. {@code E} is set to the smallest value
	 * possible without making {@code M} greater than or equal to 1.0.
	 * </p>
	 * <p>
	 * Each centimal digit of the mantissa is stored in a byte.
	 * </p>
	 * <p>
	 * If {@code E > 10}, then this routine writes of {@code E} as a
	 * varint followed by the mantissa as described above. Otherwise, if
	 * {@code E <= 10}, this routine only writes the mantissa and leaves
	 * the {@code E} value to be encoded as part of the opening byte of the
	 * field by the calling function.
	 *
	 * <pre>
	 *   Encoding:  M       (if E<=10)
	 *              E M     (if E>10)
	 * </pre>
	 * </p>
	 * @param target The destination to which encoded digits are written.
	 * @param val The value to encode.
	 * @return the number of bytes written.
	 */
	private int encodeBigDecimalLarge(DataOutputView target, BigDecimal val, Order ord) throws IOException {
		BigDecimal abs = val.abs();
		boolean isNeg = val.signum() == -1;
		int e = 0;
		int length = 0;

		// normalize abs(val) to determine E
		while (abs.compareTo(E32) >= 0 && e <= 350) {
			abs = abs.movePointLeft(32);
			e += 16;
		}
		while (abs.compareTo(E8) >= 0 && e <= 350) {
			abs = abs.movePointLeft(8);
			e += 4;
		}
		while (abs.compareTo(BigDecimal.ONE) >= 0 && e <= 350) {
			abs = abs.movePointLeft(2);
			e++;
		}

		Order compOrd = getCompositeOrder(isNeg ? Order.DESCENDING : Order.ASCENDING, ord);
		// encode appropriate header byte and/or E value.
		// negative values encoded as ~E
		if (e > 10) { /* large number, write out {~,}E */
			if (isNeg) { /* Large negative number: 0x08, ~E, ~M */
				target.writeByte(ord.apply(NEG_LARGE));
			} else { /* Large positive number: 0x22, E, M */
				target.writeByte(ord.apply(POS_LARGE));
			}
			length++;
			length += encodeInt(target, e, compOrd);
		} else {
			if (isNeg) { /* Medium negative number: 0x13-E, ~M */
				target.writeByte(ord.apply((byte) (NEG_MED_MAX - e)));
			} else { /* Medium positive number: 0x17+E, M */
				target.writeByte(ord.apply((byte) (POS_MED_MIN + e)));
			}
			length++;
		}

		// encode M by peeling off centimal digits
		// TODO: 18 is an arbitrary encoding limit. Reevaluate once we have a better handling of
		// numeric scale.
		for (int i = 0; i < 18 && abs.compareTo(BigDecimal.ZERO) != 0; i++) {
			abs = abs.movePointRight(2);
			int d = abs.intValue();
			target.writeByte(compOrd.apply((byte) (d & 0xff)));
			abs = abs.subtract(BigDecimal.valueOf(d));
			length++;
		}
		return length;
	}

	/**
	 * Decode a {@link BigDecimal} from {@code source}. Assumes {@code source} encodes
	 * a value in Numeric encoding and is within the valid range of
	 * {@link BigDecimal} values.
	 */
	public BigDecimal decodeBigDecimal(DataInputView source, Order ord) throws IOException {
		final int e;
		byte header = ord.apply(source.readByte());

		if (header == NEG_LARGE) { /* Large negative number: 0x08, ~E, ~M */
			e = decodeInt(source, getOppositeOrder(ord));
			return decodeSignificand(source, e, getOppositeOrder(ord)).negate();
		}
		if (header >= NEG_MED_MIN && header <= NEG_MED_MAX) {
			/* Medium negative number: 0x13-E, ~M */
			e = NEG_MED_MAX - header;
			return decodeSignificand(source, e, getOppositeOrder(ord)).negate();
		}
		if (header == NEG_SMALL) { /* Small negative number: 0x14, -E, ~M */
			e = -decodeInt(source, ord);
			return decodeSignificand(source, e, getOppositeOrder(ord)).negate();
		}
		if (header == ZERO) {
			return BigDecimal.ZERO;
		}
		if (header == POS_SMALL) { /* Small positive number: 0x16, ~-E, M */
			e = -decodeInt(source, getOppositeOrder(ord));
			return decodeSignificand(source, e, ord);
		}
		if (header >= POS_MED_MIN && header <= POS_MED_MAX) {
			/* Medium positive number: 0x17+E, M */
			e = header - POS_MED_MIN;
			return decodeSignificand(source, e, ord);
		}
		if (header == POS_LARGE) { /* Large positive number: 0x22, E, M */
			e = decodeInt(source, ord);
			return decodeSignificand(source, e, ord);
		}
		throw new IllegalArgumentException("unexpected value in first byte: 0x"
			+ Long.toHexString(header));
	}

	/**
	 * Read significand digits from {@code source} according to the magnitude
	 * of {@code e}.
	 * @param source The source from which to read encoded digits.
	 * @param e The magnitude of the first digit read.
	 * @return The decoded value.
	 */
	private BigDecimal decodeSignificand(DataInputView source, int e, Order ord) throws IOException {
		BigDecimal m = BigDecimal.ZERO;
		e--;

		if (reuseBuffer == null) {
			reuseBuffer = new byte[BUFFER_SIZE];
		}
		int len;
		while ((len = source.read(reuseBuffer)) != -1) {
			ord.apply(reuseBuffer, 0, len);
			for (int i = 0; i < len; i++) {
				m = m.add(// m +=
					new BigDecimal(BigInteger.ONE, e * -2).multiply(// 100 ^ p * [decoded digit]
						BigDecimal.valueOf(reuseBuffer[i] & 0xff)));
				e--;
			}
		}
		return normalize(m);
	}

	private static Order getCompositeOrder(Order ord1, Order ord2) {
		if (ord1 == ord2) {
			return Order.ASCENDING;
		} else {
			return Order.DESCENDING;
		}
	}

	private static Order getOppositeOrder(Order ord) {
		if (ord == Order.DESCENDING) {
			return Order.ASCENDING;
		} else {
			return Order.DESCENDING;
		}
	}

	/**
	 * Encode a Decimal value.
	 * @param target The destination to which encoded digits are written.
	 * @param val The value to encode.
	 * @param ord The {@link Order} to respect while encoding {@code val}.
	 * @return the number of bytes written.
	 */
	public int encodeDecimal(DataOutputView target, Decimal val, Order ord) throws IOException {
		return encodeBigDecimal(target, val.toBigDecimal(), ord);
	}

	/**
	 * Decode a Decimal value.
	 */
	public Decimal decodeDecimal(DataInputView source, int precision, int scale, Order ord) throws IOException {
		BigDecimal bd = decodeBigDecimal(source, ord);
		return Decimal.fromBigDecimal(bd, precision, scale);
	}

	/**
	 * Encode a bigint value.
	 * @param target The destination to which encoded digits are written.
	 * @param val The value to encode.
	 * @param ord The {@link Order} to respect while encoding {@code val}.
	 * @return the number of bytes written.
	 */
	public int encodeBigInteger(DataOutputView target, BigInteger val, Order ord) throws IOException {
		int sig = val.signum();
		byte[] valByteArray = val.toByteArray();
		int length = encodeInt(target, sig == -1 ? -valByteArray.length : valByteArray.length, ord);
		target.write(ord.apply(valByteArray));
		return length + valByteArray.length;
	}

	/**
	 * Decode a bigint value.
	 */
	public BigInteger decodeBigInteger(DataInputView source, Order ord) throws IOException {
		// skip the first 4 bytes which is only used for sort
		source.skipBytes(4);

		if (reuseBuffer == null) {
			reuseBuffer = new byte[BUFFER_SIZE];
		}
		if (reuseBaos == null) {
			reuseBaos = new ByteArrayOutputStreamWithPos();
		}
		reuseBaos.reset();
		int len;
		while ((len = source.read(reuseBuffer)) != -1) {
			reuseBaos.write(reuseBuffer, 0, len);
		}
		return new BigInteger(ord.apply(reuseBaos.toByteArray()));
	}

	/**
	 * Encode a BinaryString value.
	 * @param target The destination to which the encoded value is written.
	 * @param val The value to encode.
	 * @param ord The {@link Order} to respect while encoding {@code val}.
	 * @return the number of bytes written.
	 */
	public int encodeBinaryString(DataOutputView target, BinaryString val, Order ord) throws IOException {
		byte[] copy;
		if (ord == Order.ASCENDING) {
			copy = val.getBytes();
		} else {
			// need to make a copy to
			copy = new byte[val.numBytes()];
			val.copyTo(copy);
		}
		target.write(ord.apply(copy));
		int len = copy.length;
		if (ord == Order.DESCENDING) {
			target.writeByte(ord.apply(TERM));
			len += 1;
		}
		return len;
	}

	/**
	 * Decode a BinaryString value.
	 */
	public BinaryString decodeBinaryString(DataInputView source, Order ord) throws IOException {
		if (reuseBuffer == null) {
			reuseBuffer = new byte[BUFFER_SIZE];
		}
		if (reuseBaos == null) {
			reuseBaos = new ByteArrayOutputStreamWithPos();
		}
		reuseBaos.reset();
		int len;
		while ((len = source.read(reuseBuffer)) != -1) {
			reuseBaos.write(reuseBuffer, 0, len);
		}

		if (ord == Order.DESCENDING) {
			// DESCENDING ordered string requires a termination bit to preserve
			// sort-order semantics of empty values.
			reuseBaos.setPosition(reuseBaos.size() - 1);
		}
		return BinaryString.fromBytes(ord.apply(reuseBaos.toByteArray()));
	}

	/**
	 * Encode a String value.
	 * @param target The destination to which the encoded value is written.
	 * @param val The value to encode.
	 * @param ord The {@link Order} to respect while encoding {@code val}.
	 * @return the number of bytes written.
	 */
	public int encodeString(DataOutputView target, String val, Order ord) throws IOException {
		byte[] bytes = val.getBytes(UTF8);
		target.write(ord.apply(bytes));
		int len = bytes.length;
		if (ord == Order.DESCENDING) {
			target.writeByte(ord.apply(TERM));
			len += 1;
		}
		return len;
	}

	/**
	 * Decode a String value.
	 */
	public String decodeString(DataInputView source, Order ord) throws IOException {
		if (reuseBuffer == null) {
			reuseBuffer = new byte[BUFFER_SIZE];
		}
		if (reuseBaos == null) {
			reuseBaos = new ByteArrayOutputStreamWithPos();
		}
		reuseBaos.reset();
		int len;
		while ((len = source.read(reuseBuffer)) != -1) {
			reuseBaos.write(reuseBuffer, 0, len);
		}

		if (ord == Order.DESCENDING) {
			// DESCENDING ordered string requires a termination bit to preserve
			// sort-order semantics of empty values.
			reuseBaos.setPosition(reuseBaos.size() - 1);
		}
		return new String(ord.apply(reuseBaos.toByteArray()), UTF8);
	}

	/**
	 * Encode a byte array as a byte-for-byte copy.
	 * @return the number of bytes written.
	 */
	public int encodeByteArray(DataOutputView target, byte[] val, int voff, int vlen, Order ord)
		throws IOException {
		if (ord == Order.ASCENDING) {
			target.write(val, voff, vlen);
			return vlen;
		} else {
			if (reuseBuffer == null) {
				reuseBuffer = new byte[BUFFER_SIZE];
			}
			int remaining = vlen;
			while (remaining > BUFFER_SIZE) {
				System.arraycopy(val, voff, reuseBuffer, 0, BUFFER_SIZE);
				ord.apply(reuseBuffer, 0, BUFFER_SIZE);
				target.write(reuseBuffer, 0, BUFFER_SIZE);
				voff += BUFFER_SIZE;
				remaining -= BUFFER_SIZE;
			}
			System.arraycopy(val, voff, reuseBuffer, 0, remaining);
			ord.apply(reuseBuffer, 0, remaining);
			target.write(reuseBuffer, 0, remaining);
			target.writeByte(ord.apply(TERM));
			return vlen + 1;
		}

	}

	/**
	 * Encode a Blob value as a byte-for-byte copy.
	 */
	public int encodeByteArray(DataOutputView target, byte[] val, Order ord) throws IOException {
		return encodeByteArray(target, val, 0, val.length, ord);
	}

	/**
	 * Decode a byte array, byte-for-byte copy.
	 */
	public byte[] decodeByteArray(DataInputView source, Order ord) throws IOException {
		if (reuseBuffer == null) {
			reuseBuffer = new byte[BUFFER_SIZE];
		}
		if (reuseBaos == null) {
			reuseBaos = new ByteArrayOutputStreamWithPos();
		}
		reuseBaos.reset();
		int len;
		while ((len = source.read(reuseBuffer)) != -1) {
			reuseBaos.write(reuseBuffer, 0, len);
		}
		if (ord == Order.DESCENDING) {
			// DESCENDING ordered ByteArray requires a termination bit to preserve
			// sort-order semantics of empty values.
			reuseBaos.setPosition(reuseBaos.size() - 1);
		}
		return ord.apply(reuseBaos.toByteArray());
	}

	/**
	 * Encode an {@code byte} value.
	 * @return the number of bytes written.
	 */
	public int encodeByte(DataOutputView target, byte val, Order ord) throws IOException {
		byte ret = (byte) (val ^ 0x80);
		target.writeByte(ord.apply(ret));
		return 1;
	}

	/**
	 * Decode an {@code byte} value.
	 */
	public byte decodeByte(DataInputView source, Order ord) throws IOException {
		return (byte) ((ord.apply(source.readByte()) ^ 0x80) & 0xff);
	}

	/**
	 * Encode an {@code short} value.
	 * @return the number of bytes written.
	 */
	public int encodeShort(DataOutputView target, short val, Order ord) throws IOException {
		target.writeByte(ord.apply((byte) ((val >> 8) ^ 0x80)));
		target.writeByte(ord.apply((byte) val));
		return 2;
	}

	/**
	 * Decode an {@code short} value.
	 */
	public short decodeShort(DataInputView source, Order ord) throws IOException {
		short val = (short) ((ord.apply(source.readByte()) ^ 0x80) & 0xff);
		val = (short) ((val << 8) | (ord.apply(source.readByte()) & 0xff));
		return val;
	}

	/**
	 * Encode an {@code int} value.
	 * @return the number of bytes written.
	 */
	public int encodeInt(DataOutputView target, int val, Order ord) throws IOException {
		target.writeByte(ord.apply((byte) ((val >> 24) ^ 0x80)));
		target.writeByte(ord.apply((byte) (val >> 16)));
		target.writeByte(ord.apply((byte) (val >> 8)));
		target.writeByte(ord.apply((byte) val));
		return 4;
	}

	/**
	 * Decode an {@code int} value.
	 */
	public int decodeInt(DataInputView source, Order ord) throws IOException {
		int val = (ord.apply(source.readByte()) ^ 0x80) & 0xff;
		for (int i = 1; i < 4; i++) {
			val = (val << 8) | (ord.apply(source.readByte()) & 0xff);
		}
		return val;
	}

	/**
	 * Encode an {@code long} value.
	 * <p>
	 * This format ensures that all longs sort in their natural order, as they
	 * would sort when using signed long comparison.
	 * </p>
	 * <p>
	 * All Longs are serialized to an 8-byte, fixed-width sortable byte format.
	 * Serialization is performed by inverting the integer sign bit and writing
	 * the resulting bytes to the byte array in big endian order. This encoding
	 * is designed to handle java language primitives and so Null values are NOT
	 * supported by this implementation.
	 * </p>
	 * <p>
	 * For example:
	 * </p>
	 * <pre>
	 * Input:   0x0000000000000005 (5)
	 * Result:  0x8000000000000005
	 *
	 * Input:   0xfffffffffffffffb (-4)
	 * Result:  0x0000000000000004
	 *
	 * Input:   0x7fffffffffffffff (Long.MAX_VALUE)
	 * Result:  0xffffffffffffffff
	 *
	 * Input:   0x8000000000000000 (Long.MIN_VALUE)
	 * Result:  0x0000000000000000
	 * </pre>
	 * @return the number of bytes written.
	 */
	public int encodeLong(DataOutputView target, long val, Order ord) throws IOException {
		target.writeByte(ord.apply((byte) ((val >> 56) ^ 0x80)));
		target.writeByte(ord.apply((byte) (val >> 48)));
		target.writeByte(ord.apply((byte) (val >> 40)));
		target.writeByte(ord.apply((byte) (val >> 32)));
		target.writeByte(ord.apply((byte) (val >> 24)));
		target.writeByte(ord.apply((byte) (val >> 16)));
		target.writeByte(ord.apply((byte) (val >> 8)));
		target.writeByte(ord.apply((byte) val));
		return 8;
	}

	/**
	 * Decode an {@code long} value.
	 */
	public long decodeLong(DataInputView source, Order ord) throws IOException {
		long val = (ord.apply(source.readByte()) ^ 0x80) & 0xff;
		for (int i = 1; i < 8; i++) {
			val = (val << 8) | (ord.apply(source.readByte()) & 0xff);
		}
		return val;
	}

	/**
	 * Encode a 32-bit floating point value.
	 * @return the number of bytes written.
	 */
	public int encodeFloat(DataOutputView target, float val, Order ord) throws IOException {
		int i = Float.floatToIntBits(val);
		i ^= ((i >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
		target.writeByte(ord.apply((byte) (i >> 24)));
		target.writeByte(ord.apply((byte) (i >> 16)));
		target.writeByte(ord.apply((byte) (i >> 8)));
		target.writeByte(ord.apply((byte) i));
		return 4;
	}

	/**
	 * Decode a 32-bit floating point value.
	 */
	public float decodeFloat(DataInputView source, Order ord) throws IOException {
		int val = ord.apply(source.readByte()) & 0xff;
		for (int i = 1; i < 4; i++) {
			val = (val << 8) | (ord.apply(source.readByte()) & 0xff);
		}
		val ^= (~val >> (Integer.SIZE - 1)) | Integer.MIN_VALUE;
		return Float.intBitsToFloat(val);
	}

	/**
	 * Encode a 64-bit floating point value.
	 * <p>
	 * This format ensures the following total ordering of floating point
	 * values: Double.NEGATIVE_INFINITY &lt; -Double.MAX_VALUE &lt; ... &lt;
	 * -Double.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Double.MIN_VALUE &lt; ...
	 * &lt; Double.MAX_VALUE &lt; Double.POSITIVE_INFINITY &lt; Double.NaN
	 * </p>
	 * <p>
	 * Floating point numbers are encoded as specified in IEEE 754. A 64-bit
	 * double precision float consists of a sign bit, 11-bit unsigned exponent
	 * encoded in offset-1023 notation, and a 52-bit significand. The format is
	 * described further in the <a
	 * href="http://en.wikipedia.org/wiki/Double_precision"> Double Precision
	 * Floating Point Wikipedia page</a> </p>
	 * <p>
	 * The value of a normal float is -1 <sup>sign bit</sup> &times;
	 * 2<sup>exponent - 1023</sup> &times; 1.significand
	 * </p>
	 * <p>
	 * The IEE754 floating point format already preserves sort ordering for
	 * positive floating point numbers when the raw bytes are compared in most
	 * significant byte order. This is discussed further at <a href=
	 * "http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm"
	 * > http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.
	 * htm</a>
	 * </p>
	 * <p>
	 * Thus, we need only ensure that negative numbers sort in the the exact
	 * opposite order as positive numbers (so that say, negative infinity is
	 * less than negative 1), and that all negative numbers compare less than
	 * any positive number. To accomplish this, we invert the sign bit of all
	 * floating point numbers, and we also invert the exponent and significand
	 * bits if the floating point number was negative.
	 * </p>
	 * <p>
	 * More specifically, we first store the floating point bits into a 64-bit
	 * long {@code l} using {@link Double#doubleToLongBits}. This method
	 * collapses all NaNs into a single, canonical NaN value but otherwise
	 * leaves the bits unchanged. We then compute
	 * </p>
	 * <pre>
	 * l &circ;= (l &gt;&gt; (Long.SIZE - 1)) | Long.MIN_SIZE
	 * </pre>
	 * <p>
	 * which inverts the sign bit and XOR's all other bits with the sign bit
	 * itself. Comparing the raw bytes of {@code l} in most significant
	 * byte order is equivalent to performing a double precision floating point
	 * comparison on the underlying bits (ignoring NaN comparisons, as NaNs
	 * don't compare equal to anything when performing floating point
	 * comparisons).
	 * </p>
	 * <p>
	 * The resulting long integer is then converted into a byte array by
	 * serializing the long one byte at a time in most significant byte order.
	 * The serialized integer is prefixed by a single header byte. All
	 * serialized values are 9 bytes in length.
	 * </p>
	 * @return the number of bytes written.
	 */
	public int encodeDouble(DataOutputView target, double val, Order ord) throws IOException {
		long lng = Double.doubleToLongBits(val);
		lng ^= ((lng >> (Long.SIZE - 1)) | Long.MIN_VALUE);
		target.writeByte(ord.apply((byte) (lng >> 56)));
		target.writeByte(ord.apply((byte) (lng >> 48)));
		target.writeByte(ord.apply((byte) (lng >> 40)));
		target.writeByte(ord.apply((byte) (lng >> 32)));
		target.writeByte(ord.apply((byte) (lng >> 24)));
		target.writeByte(ord.apply((byte) (lng >> 16)));
		target.writeByte(ord.apply((byte) (lng >> 8)));
		target.writeByte(ord.apply((byte) lng));
		return 8;
	}

	/**
	 * Decode a 64-bit floating point value.
	 */
	public double decodeDouble(DataInputView source, Order ord) throws IOException {
		long val = ord.apply(source.readByte()) & 0xff;
		for (int i = 1; i < 8; i++) {
			val = (val << 8) + (ord.apply(source.readByte()) & 0xff);
		}
		val ^= (~val >> (Long.SIZE - 1)) | Long.MIN_VALUE;
		return Double.longBitsToDouble(val);
	}

	/**
	 * Encode an {@code char} value.
	 * @return the number of bytes written.
	 */
	public int encodeChar(DataOutputView target, char val, Order ord) throws IOException {
		target.writeByte(ord.apply((byte) (val >> 8)));
		target.writeByte(ord.apply((byte) val));
		return 2;
	}

	/**
	 * Decode an {@code char} value.
	 */
	public char decodeChar(DataInputView source, Order ord) throws IOException {
		char val = (char) (ord.apply(source.readByte()) & 0xff);
		val = (char) ((val << 8) | (ord.apply(source.readByte()) & 0xff));
		return val;
	}

	/**
	 * Used to describe or modify the lexicographical sort order of a
	 * {@code byte[]}. Default ordering is considered {@code ASCENDING}. The order
	 * of a {@code byte[]} can be inverted, resulting in {@code DESCENDING} order,
	 * by replacing each byte with its 1's compliment.
	 */
	public enum Order {

		ASCENDING {
			@Override
			public int cmp(int cmp) {
				/* noop */
				return cmp;
			}

			@Override
			public byte apply(byte val) {
				/* noop */
				return val;
			}

			@Override
			public byte[] apply(byte[] val) {
				/* noop */
				return val;
			}

			@Override
			public void apply(byte[] val, int offset, int length) {
				/* noop */
			}

			@Override
			public String toString() {
				return "ASCENDING";
			}
		},

		DESCENDING {
			/**
			 * A {@code byte} value is inverted by taking its 1's Complement, achieved
			 * via {@code xor} with {@code 0xff}.
			 */
			private static final byte MASK = (byte) 0xff;

			@Override
			public int cmp(int cmp) {
				return -1 * cmp;
			}

			@Override
			public byte apply(byte val) {
				return (byte) (val ^ MASK);
			}

			@Override
			public byte[] apply(byte[] val) {
				for (int i = 0; i < val.length; i++) {
					val[i] ^= MASK;
				}
				return val;
			}

			@Override
			public void apply(byte[] val, int offset, int length) {
				for (int i = 0; i < length; i++) {
					val[offset + i] ^= MASK;
				}
			}

			@Override
			public String toString() {
				return "DESCENDING";
			}
		};

		/**
		 * Returns the adjusted trichotomous value according to the ordering imposed by this
		 * {@code Order}.
		 */
		public abstract int cmp(int cmp);

		/**
		 * Apply order to the byte {@code val}.
		 */
		public abstract byte apply(byte val);

		/**
		 * Apply order to the byte array {@code val}.
		 */
		public abstract byte[] apply(byte[] val);

		/**
		 * Apply order to a range within the byte array {@code val}.
		 */
		public abstract void apply(byte[] val, int offset, int length);
	}
}


