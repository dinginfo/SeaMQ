/*
 * Copyright 2016 David Ding.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.dinginfo.seamq;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

public class DataField implements Serializable {
	private static final String UTF8_ENCODING = "UTF-8";

	private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
	/**
	 * Size of boolean in bytes
	 */
	public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

	/**
	 * Size of byte in bytes
	 */
	public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

	/**
	 * Size of char in bytes
	 */
	public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

	/**
	 * Size of double in bytes
	 */
	public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

	/**
	 * Size of float in bytes
	 */
	public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

	/**
	 * Size of int in bytes
	 */
	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

	/**
	 * Size of long in bytes
	 */
	public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

	/**
	 * Size of short in bytes
	 */
	public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

	/**
	 * Mask to apply to a long to reveal the lower int only. Use like this: int
	 * i = (int)(0xFFFFFFFF00000000l ^ some_long_value);
	 */
	public static final long MASK_FOR_LOWER_INT_IN_LONG = 0xFFFFFFFF00000000l;

	public static final short TYPE_BYTE = 0;

	public static final short TYPE_STRING = 1;

	public static final short TYPE_INT = 2;

	public static final short TYPE_LONG = 3;

	public static final short TYPE_FLOAT = 4;

	public static final short TYPE_DOUBLE = 5;

	public static final short TYPE_SHORT = 6;

	public static final short TYPE_BOOLEAN = 7;

	public static final short TYPE_BIGDECIMAL = 8;

	private String key;

	private short dataType;

	private byte[] data;

	public DataField(String key, short dataType, byte[] data) {
		this.key = key;
		this.dataType = dataType;
		this.data = data;
	}
	
	public DataField(String key, int dataType, byte[] data) {
		this.key = key;
		Integer type = dataType;
		this.dataType = type.shortValue();
		this.data = data;
	}

	public DataField(String key, byte[] data) {
		this.key = key;
		this.dataType = TYPE_BYTE;
		this.data = data;
	}
	
	public DataField(String key){
		this.key = key;
	}

	public DataField(String key, int value) {
		this.key = key;
		setInt(value);
	}
	
	public DataField(String key, String value) {
		this.key = key;
		setString(value);
	}

	public DataField(String key,boolean value){
		this.key = key;
		setBoolean(value);
	}
	

	public String getKey() {
		return key;
	}

	public short getDataType() {
		return dataType;
	}

	public byte[] getData() {
		return data;
	}
	
	public int getSize(){
		int size = 2;
		if(key!=null){
			size = size + key.length();
		} 
		if(data!=null){
			size = size + data.length;
		}
		return size;
	}

	public void setBigDecimal(BigDecimal val) {
		byte[] valueBytes = val.unscaledValue().toByteArray();
		byte[] result = new byte[valueBytes.length + SIZEOF_INT];
		int offset = putInt(result, 0, val.scale());
		putBytes(result, offset, valueBytes, 0, valueBytes.length);
		data = result;
		dataType = TYPE_BIGDECIMAL;
	}

	private int putInt(byte[] bytes, int offset, int val) {
		if (bytes.length - offset < SIZEOF_INT) {
			throw new IllegalArgumentException(
					"Not enough room to put an int at" + " offset " + offset
							+ " in a " + bytes.length + " byte array");
		}
		for (int i = offset + 3; i > offset; i--) {
			bytes[i] = (byte) val;
			val >>>= 8;
		}
		bytes[offset] = (byte) val;
		return offset + SIZEOF_INT;
	}

	private static int putBytes(byte[] tgtBytes, int tgtOffset,
			byte[] srcBytes, int srcOffset, int srcLength) {
		System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
		return tgtOffset + srcLength;
	}

	public void setBoolean(final boolean b) {
		data = new byte[] { b ? (byte) -1 : (byte) 0 };
		dataType = TYPE_BOOLEAN;

	}

	public void setDouble(final double d) {
		data = toBytes(Double.doubleToRawLongBits(d));
		dataType = TYPE_DOUBLE;
	}

	public void setLong(long val) {
		data = toBytes(val);
		dataType = TYPE_LONG;
	}

	private byte[] toBytes(long val) {
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public void setFloat(final float f) {

		data = toBytes(Float.floatToRawIntBits(f));
		dataType = TYPE_FLOAT;
	}

	public void setInt(int val) {
		data = toBytes(val);
		dataType = TYPE_INT;
	}

	private byte[] toBytes(int val) {
		byte[] b = new byte[4];
		for (int i = 3; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public void setShort(short val) {
		byte[] b = new byte[SIZEOF_SHORT];
		b[1] = (byte) val;
		val >>= 8;
		b[0] = (byte) val;
		data = b;
		dataType = TYPE_SHORT;
	}

	public void setString(String s) {
		data = s.getBytes(UTF8_CHARSET);
		dataType = TYPE_STRING;
	}

	public double getDouble() {
		return toDouble(data, 0);
	}

	private double toDouble(final byte[] bytes, final int offset) {
		return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
	}

	public long getLong() {
		return toLong(data, 0, SIZEOF_LONG);
	}

	private long toLong(byte[] bytes, int offset, final int length) {
		if (length != SIZEOF_LONG || offset + length > bytes.length) {
			throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
		}
		long l = 0;
		for (int i = offset; i < offset + length; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

	private static IllegalArgumentException explainWrongLengthOrOffset(
			final byte[] bytes, final int offset, final int length,
			final int expectedLength) {
		String reason;
		if (length != expectedLength) {
			reason = "Wrong length: " + length + ", expected " + expectedLength;
		} else {
			reason = "offset (" + offset + ") + length (" + length
					+ ") exceed the" + " capacity of the array: "
					+ bytes.length;
		}
		return new IllegalArgumentException(reason);
	}

	public short getShort() {
		return toShort(data, 0, SIZEOF_SHORT);
	}

	/**
	 * Converts a byte array to a short value
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset into array
	 * @return the short value
	 */
	private short toShort(byte[] bytes, int offset) {
		return toShort(bytes, offset, SIZEOF_SHORT);
	}

	/**
	 * Converts a byte array to a short value
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset into array
	 * @param length
	 *            length, has to be {@link #SIZEOF_SHORT}
	 * @return the short value
	 * @throws IllegalArgumentException
	 *             if length is not {@link #SIZEOF_SHORT} or if there's not
	 *             enough room in the array at the offset indicated.
	 */
	private short toShort(byte[] bytes, int offset, final int length) {
		if (length != SIZEOF_SHORT || offset + length > bytes.length) {
			throw explainWrongLengthOrOffset(bytes, offset, length,
					SIZEOF_SHORT);
		}

		short n = 0;
		n ^= bytes[offset] & 0xFF;
		n <<= 8;
		n ^= bytes[offset + 1] & 0xFF;
		return n;
	}

	public String getString() {
		if (data == null) {
			return null;
		}
		return toString(data, 0, data.length);
	}

	private String toString(final byte[] b1, String sep, final byte[] b2) {
		return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
	}

	private String toString(final byte[] b, int off) {
		if (b == null) {
			return null;
		}
		int len = b.length - off;
		if (len <= 0) {
			return "";
		}
		return new String(b, off, len, UTF8_CHARSET);
	}

	private String toString(final byte[] b, int off, int len) {
		if (b == null) {
			return null;
		}
		if (len == 0) {
			return "";
		}
		return new String(b, off, len, UTF8_CHARSET);
	}

	public int getInt() {
		return toInt(data, 0, SIZEOF_INT);
	}

	private int toInt(byte[] bytes, int offset) {
		return toInt(bytes, offset, SIZEOF_INT);
	}

	private int toInt(byte[] bytes, int offset, final int length) {
		if (length != SIZEOF_INT || offset + length > bytes.length) {
			throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
		}

		int n = 0;
		for (int i = offset; i < (offset + length); i++) {
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}

	public float getFloat() {
		return Float.intBitsToFloat(toInt(data, 0, SIZEOF_INT));
	}

	public boolean isBoolean() {
		if (data.length != 1) {
			throw new IllegalArgumentException("Array has wrong size: "
					+ data.length);
		}
		return data[0] != (byte) 0;
	}

	public BigDecimal getBigDecimal() {
		return toBigDecimal(data, 0, data.length);
	}

	
	private BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
		if (bytes == null || length < SIZEOF_INT + 1
				|| (offset + length > bytes.length)) {
			return null;
		}

		int scale = toInt(bytes, offset);
		byte[] tcBytes = new byte[length - SIZEOF_INT];
		System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length
				- SIZEOF_INT);
		return new BigDecimal(new BigInteger(tcBytes), scale);
	}
}
