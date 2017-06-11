package org.apache.flink.table.api.java.utils;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * All pojos for table api testing.
 */
public class Pojos {

	/**
	 * Pojo0 for test.
	 */
	public static class Pojo0 implements Serializable {
		private Timestamp winStart;
		private Timestamp winEnd;
		private String name;
		private Long cnt;

		public Timestamp getWinStart() {
			return winStart;
		}

		public void setWinStart(Timestamp winStart) {
			this.winStart = winStart;
		}

		public Timestamp getWinEnd() {
			return winEnd;
		}

		public void setWinEnd(Timestamp winEnd) {
			this.winEnd = winEnd;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Long getCnt() {
			return cnt;
		}

		public void setCnt(Long cnt) {
			this.cnt = cnt;
		}

		@Override
		public String toString() {
			return "Pojo0{" +
				"winStart=" + winStart +
				", winEnd=" + winEnd +
				", name='" + name + '\'' +
				", cnt=" + cnt +
				'}';
		}
	}

	/**
	 * Pojo1 for test.
	 */
	public static class Pojo1 implements Serializable {
		private Timestamp id;
		private String name;
		private Long myCnt;
		private Long mySum;

		public Timestamp getId() {
			return id;
		}

		public void setId(Timestamp id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Long getMyCnt() {
			return myCnt;
		}

		public void setMyCnt(Long myCnt) {
			this.myCnt = myCnt;
		}

		public Long getMySum() {
			return mySum;
		}

		public void setMySum(Long mySum) {
			this.mySum = mySum;
		}

		@Override
		public String toString() {
			return "Pojo1{" +
				"id=" + id +
				", name='" + name + '\'' +
				", myCnt=" + myCnt +
				", mySum=" + mySum +
				'}';
		}
	}

	/**
	 * Pojo2 for test.
	 */
	public static class Pojo2 implements Serializable {
		private String string;
		private Long myCnt;
		private Integer myAvg;
		private Long myWeightAvg;
		private Integer myMin;
		private Integer myMax;
		private Integer mySum;
		private Timestamp winStart;
		private Timestamp winEnd;

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		public Long getMyCnt() {
			return myCnt;
		}

		public void setMyCnt(Long myCnt) {
			this.myCnt = myCnt;
		}

		public Integer getMyAvg() {
			return myAvg;
		}

		public void setMyAvg(Integer myAvg) {
			this.myAvg = myAvg;
		}

		public Long getMyWeightAvg() {
			return myWeightAvg;
		}

		public void setMyWeightAvg(Long myWeightAvg) {
			this.myWeightAvg = myWeightAvg;
		}

		public Integer getMyMin() {
			return myMin;
		}

		public void setMyMin(Integer myMin) {
			this.myMin = myMin;
		}

		public Integer getMyMax() {
			return myMax;
		}

		public void setMyMax(Integer myMax) {
			this.myMax = myMax;
		}

		public Integer getMySum() {
			return mySum;
		}

		public void setMySum(Integer mySum) {
			this.mySum = mySum;
		}

		public Timestamp getWinStart() {
			return winStart;
		}

		public void setWinStart(Timestamp winStart) {
			this.winStart = winStart;
		}

		public Timestamp getWinEnd() {
			return winEnd;
		}

		public void setWinEnd(Timestamp winEnd) {
			this.winEnd = winEnd;
		}

		@Override
		public String toString() {
			return "Pojo2{" +
				"string='" + string + '\'' +
				", myCnt=" + myCnt +
				", myAvg=" + myAvg +
				", myWeightAvg=" + myWeightAvg +
				", myMin=" + myMin +
				", myMax=" + myMax +
				", mySum=" + mySum +
				", winStart=" + winStart +
				", winEnd=" + winEnd +
				'}';
		}
	}

	/**
	 * Pojo3 for test.
	 */
	public static class Pojo3 implements Serializable {
		private String myMsg;
		private Timestamp myTs;
		private Long myCnt;
		private Long mySum;

		public String getMyMsg() {
			return myMsg;
		}

		public void setMyMsg(String myMsg) {
			this.myMsg = myMsg;
		}

		public Timestamp getMyTs() {
			return myTs;
		}

		public void setMyTs(Timestamp myTs) {
			this.myTs = myTs;
		}

		public Long getMyCnt() {
			return myCnt;
		}

		public void setMyCnt(Long myCnt) {
			this.myCnt = myCnt;
		}

		public Long getMySum() {
			return mySum;
		}

		public void setMySum(Long mySum) {
			this.mySum = mySum;
		}

		@Override
		public String toString() {
			return "Pojo3{" +
				"myMsg='" + myMsg + '\'' +
				", myTs=" + myTs +
				", myCnt=" + myCnt +
				", mySum=" + mySum +
				'}';
		}
	}
}
