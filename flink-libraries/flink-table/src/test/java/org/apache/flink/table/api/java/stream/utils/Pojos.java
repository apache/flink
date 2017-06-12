package org.apache.flink.table.api.java.stream.utils;

import java.io.Serializable;

/**
 *  Pojos for testing.
 */
public class Pojos {
	/**
	 * Pojo0 for testing.
	 */
	public static class Pojo0 implements Serializable {

		private Integer myInt;
		private Long myLong;
		private Long myLong2;
		private String myString;

		public Integer getMyInt() {
			return myInt;
		}

		public void setMyInt(Integer myInt) {
			this.myInt = myInt;
		}

		public Long getMyLong() {
			return myLong;
		}

		public void setMyLong(Long myLong) {
			this.myLong = myLong;
		}

		public Long getMyLong2() {
			return myLong2;
		}

		public void setMyLong2(Long myLong2) {
			this.myLong2 = myLong2;
		}

		public String getMyString() {
			return myString;
		}

		public void setMyString(String myString) {
			this.myString = myString;
		}

		@Override
		public String toString() {
			return "Pojo0{" +
				"myInt=" + myInt +
				", myLong=" + myLong +
				", myLong2=" + myLong2 +
				", myString='" + myString + '\'' +
				'}';
		}
	}
}
