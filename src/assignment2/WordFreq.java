package assignment2;

import java.io.*;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordFreq {

	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			line = line.replaceAll(",", " ");
			line = line.replaceAll("\\.", " ");
			line = line.replaceAll("-", " ");
			StringTokenizer tokenizer = new StringTokenizer(line);
			int movieId = Integer.parseInt(tokenizer.nextToken());
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken().toLowerCase();
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum++;
			}
			context.write( key, new IntWritable(sum));
		}
	}

	public static class CountMapperII extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String w = tokenizer.nextToken();
			int num = Integer.parseInt(tokenizer.nextToken());
			context.write(new IntWritable(num), new Text(w));
		}
	}
	
	public static class CountReducerII extends Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(new Text(val.toString()), key);
			}
		}
	}
	
	public static class customComp extends Comparator {
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -1 * super.compare(a, b);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -1 * super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	
	public static void main(String[] args) throws Exception {
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordFreq.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setJarByClass(WordFreq.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileSystem fs = FileSystem.get(conf);
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+"temp1");
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileOutputFormat.setOutputPath(job, outputDestination);
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;

		// job.submit();
		
		int jobCompletionStatus2 = 99;
		
		if(jobCompletionStatus == 0){
			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2);
			job2.setJarByClass(WordFreq.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			if (args.length > 2){
				job2.setNumReduceTasks(Integer.parseInt(args[2]));
			}
	
			job2.setMapperClass(CountMapperII.class);
			job2.setReducerClass(CountReducerII.class);
			//job2.setJarByClass(WordFreq.class);
			job2.setSortComparatorClass(customComp.class);
			job2.setNumReduceTasks(1);
	
			FileInputFormat.addInputPath(job2, new Path(args[0]+"temp1"));
			FileSystem fs2 = FileSystem.get(conf2);
			// handle (e.g. delete) existing output path
			Path outputDestination2 = new Path(args[0]+args[1]);
			if (fs2.exists(outputDestination2)) {
				fs2.delete(outputDestination2, true);
			}
	
			// set output path & start job1
			FileOutputFormat.setOutputPath(job2, outputDestination2);
			jobCompletionStatus2 = job2.waitForCompletion(true) ? 0 : 1;
		}
		
		if(jobCompletionStatus2 == 0){
			fs.delete(outputDestination, true);
		}
		
	}
}