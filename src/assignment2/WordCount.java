package assignment2;

import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {

	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int movieId = Integer.parseInt(tokenizer.nextToken());
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				context.write(new Text("1"), new IntWritable(1));
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum++;
			}
			context.write(new IntWritable(sum), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setJarByClass(WordCount.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileSystem fs = FileSystem.get(conf);
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileOutputFormat.setOutputPath(job, outputDestination);
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;
	}
}