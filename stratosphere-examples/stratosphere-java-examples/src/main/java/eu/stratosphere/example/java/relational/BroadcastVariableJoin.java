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
package eu.stratosphere.example.java.relational;


import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

public class BroadcastVariableJoin {
	
	public static class User {
		
		private int id;
		private String name;
		private String password;
		
		public User() {}
		
		public User(Integer id, String name, String password) {
			super();
			this.id = id;
			this.name = name;
			this.password = password;
		}
		
		public int getId() {
			return id;
		}
		
		public void setId(Integer id) {
			this.id = id;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getPassword() {
			return password;
		}
		
		public void setPassword(String password) {
			this.password = password;
		}
		
		@Override
		public String toString() {
			return "User (" + id + ", " + name + ", " + password + ")";
		}
	}
	
	public static class Comment {
		
		private int uid;
		
		private String comment;
		
		public Comment() {}
		
		public Comment(int u) {
			uid = u;
			comment = "Hello Stratosphere!";
		}
		
		public int getUid() {
			return uid;
		}
		
		public String getComment() {
			return comment;
		}
		
		@Override
		public String toString() {
			return "Comment (" + uid + ", " + comment + ")";
		}
	}
	
	public static class CommentsGenerator implements Iterator<Comment>, Serializable {
		
		private static final long serialVersionUID = 1L;
		
		private int i = 0;
		
		@Override
		public boolean hasNext() {
			return i++ < 500;
		}

		@Override
		public Comment next() {
			return new Comment(i);
		}

		@Override
		public void remove() {}
		
	}
	
	public static class ToUserObject extends MapFunction<Tuple3<Integer, String, String>, User> {

		private static final long serialVersionUID = 1L;

		@Override
		public User map(Tuple3<Integer, String, String> value) {
			return new User( value.f0, value.f1, value.f2);
		}
	}
	
	public static class HandJoin extends FlatMapFunction<User, Tuple2<User, Comment>> {

		private static final long serialVersionUID = 1L;
		
		private Collection<Comment> com;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			com = getRuntimeContext().getBroadcastVariable("comments");
		}
		@Override
		public void flatMap(User value, Collector<Tuple2<User, Comment>>  out) throws Exception {
			Iterator<Comment> it = com.iterator();
			while (it.hasNext()) {
				Comment c = it.next();
				if (c.getUid() == value.getId()) {
					out.collect(new Tuple2<User, Comment>(value, c));
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Comment> userComments = context.fromCollection(new CommentsGenerator(), Comment.class);
		
		DataSet<Tuple3<Integer, String, String>> usersTuples = context.readCsvFile("file:///home/cicero/Desktop/users.csv").types(Integer.class, String.class, String.class);
		DataSet<User> usersUsers = usersTuples.map(new ToUserObject());
		
		DataSet<Tuple2<User, Comment>> joined = usersUsers.flatMap(new HandJoin()).withBroadcastSet(userComments, "comments");
		
		joined.print();
		
		context.execute();
		
	}
}