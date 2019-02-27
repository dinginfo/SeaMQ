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
package com.dinginfo.seamq.common;

import java.math.BigDecimal;
import java.nio.charset.Charset;

public class ByteUtils {

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

	public static byte[] toBytes(BigDecimal val) {
		byte[] valueBytes = val.unscaledValue().toByteArray();
		byte[] result = new byte[valueBytes.length + SIZEOF_INT];
		int offset = putInt(result, 0, val.scale());
		putBytes(result, offset, valueBytes, 0, valueBytes.length);
		return result;
	}

	public static int putInt(byte[] bytes, int offset, int val) {
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

	public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
			int srcOffset, int srcLength) {
		System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
		return tgtOffset + srcLength;
	}

	public static byte[] toBytes(final boolean b) {
		return new byte[] { b ? (byte) -1 : (byte) 0 };
	}

	public static byte[] toBytes(final double d) {
		return ByteUtils.toBytes(Double.doubleToRawLongBits(d));
	}

	public static byte[] toBytes(long val) {
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(final float f) {

		return ByteUtils.toBytes(Float.floatToRawIntBits(f));
	}

	public static byte[] toBytes(int val) {
		byte[] b = new byte[4];
		for (int i = 3; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(short val) {
		byte[] b = new byte[SIZEOF_SHORT];
		b[1] = (byte) val;
		val >>= 8;
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(String s) {
		return s.getBytes(UTF8_CHARSET);
	}

	public static double toDouble(final byte[] bytes) {
		return toDouble(bytes, 0);
	}

	/**
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset where double is
	 * @return Return double made from passed bytes.
	 */
	public static double toDouble(final byte[] bytes, final int offset) {
		return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
	}

	public static long toLong(byte[] bytes, int offset, final int length) {
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

	public static short toShort(byte[] bytes) {
		return toShort(bytes, 0, SIZEOF_SHORT);
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
	public static short toShort(byte[] bytes, int offset) {
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
	public static short toShort(byte[] bytes, int offset, final int length) {
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

	public static String toString(final byte[] b) {
		if (b == null) {
			return null;
		}
		return toString(b, 0, b.length);
	}

	/**
	 * Joins two byte arrays together using a separator.
	 * 
	 * @param b1
	 *            The first byte array.
	 * @param sep
	 *            The separator to use.
	 * @param b2
	 *            The second byte array.
	 */
	public static String toString(final byte[] b1, String sep, final byte[] b2) {
		return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
	}

	/**
	 * This method will convert utf8 encoded bytes into a string. If the given
	 * byte array is null, this method will return null.
	 * 
	 * @param b
	 *            Presumed UTF-8 encoded byte array.
	 * @param off
	 *            offset into array
	 * @return String made from <code>b</code> or null
	 */
	public static String toString(final byte[] b, int off) {
		if (b == null) {
			return null;
		}
		int len = b.length - off;
		if (len <= 0) {
			return "";
		}
		return new String(b, off, len, UTF8_CHARSET);
	}

	/**
	 * This method will convert utf8 encoded bytes into a string. If the given
	 * byte array is null, this method will return null.
	 * 
	 * @param b
	 *            Presumed UTF-8 encoded byte array.
	 * @param off
	 *            offset into array
	 * @param len
	 *            length of utf-8 sequence
	 * @return String made from <code>b</code> or null
	 */
	public static String toString(final byte[] b, int off, int len) {
		if (b == null) {
			return null;
		}
		if (len == 0) {
			return "";
		}
		return new String(b, off, len, UTF8_CHARSET);
	}
	
	 public static int toInt(byte[] bytes) {
		    return toInt(bytes, 0, SIZEOF_INT);
		  }

		  /**
		   * Converts a byte array to an int value
		   * @param bytes byte array
		   * @param offset offset into array
		   * @return the int value
		   */
		  public static int toInt(byte[] bytes, int offset) {
		    return toInt(bytes, offset, SIZEOF_INT);
		  }

		  /**
		   * Converts a byte array to an int value
		   * @param bytes byte array
		   * @param offset offset into array
		   * @param length length of int (has to be {@link #SIZEOF_INT})
		   * @return the int value
		   * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
		   * if there's not enough room in the array at the offset indicated.
		   */
		  public static int toInt(byte[] bytes, int offset, final int length) {
		    if (length != SIZEOF_INT || offset + length > bytes.length) {
		      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
		    }
		    
		    int n = 0;
		      for(int i = offset; i < (offset + length); i++) {
		        n <<= 8;
		        n ^= bytes[i] & 0xFF;
		      }
		      return n;
		  }
}
