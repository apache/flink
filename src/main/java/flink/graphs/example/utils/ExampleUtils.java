package flink.graphs.example.utils;

import java.io.PrintStream;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class ExampleUtils {

	@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
	public static void printResult(DataSet set, String msg, ExecutionEnvironment env) {
		set.output(new PrintingOutputFormatWithMessage(msg) {
		});
	}
	
	public static class PrintingOutputFormatWithMessage<T> implements OutputFormat<T> {

		private static final long serialVersionUID = 1L;
		
		private transient PrintStream stream;
		
		private transient String prefix;
		
		private String message;
		
		// --------------------------------------------------------------------------------------------
		
		/**
		 * Instantiates a printing output format that prints to standard out.
		 */
		public PrintingOutputFormatWithMessage() {}
		
		public PrintingOutputFormatWithMessage(String msg) {
			this.message = msg;	
		}

		@Override
		public void open(int taskNumber, int numTasks) {
			// get the target stream
			this.stream = System.out;
			
			// set the prefix to message
			this.prefix = message + ": ";
		}

		@Override
		public void writeRecord(T record) {
			if (this.prefix != null) {
				this.stream.println(this.prefix + record.toString());
			}
			else {
				this.stream.println(record.toString());
			}
		}

		@Override
		public void close() {
			this.stream = null;
			this.prefix = null;
		}
		
		@Override
		public String toString() {
			return "Print to System.out";
		}

		@Override
		public void configure(Configuration parameters) {}
	}
}
