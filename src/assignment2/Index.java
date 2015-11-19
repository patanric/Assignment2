package assignment2;

import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Index {

	public static class CountMapper extends Mapper<Object, Text, WordKey, IntWritable> {

		/*
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Set<String> stopWords = new HashSet();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			File stopWordFile = null;
			if(context.getCacheFiles() != null){
				stopWordFile = new File("./stopw");
			} else {
				System.err.println("StopFile not found");
			}
			
			BufferedReader stopWordsReader = Files.newBufferedReader(stopWordFile.toPath(), StandardCharsets.UTF_8);
			String line = null;
			while((line = stopWordsReader.readLine()) != null){
				stopWords.add(line);
			}
			super.setup(context);
		}
		*/

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
				//if(!stopWords.contains(word)){
					context.write(new WordKey(new Text(word), new IntWritable(movieId)), new IntWritable(1));
				//}
				
			}
		}
	}

	public static class CountReducer extends Reducer<WordKey, IntWritable, Text, IntWritable> {
		
		public void reduce(WordKey wk, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum++;
			}
			context.write(new Text(wk.getWord().toString() + "\t" + wk.getMovieID().toString()), new IntWritable(sum));
		}
	}

	public static class CountMapperII extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] input	= line.split("\t");
			//System.out.println(line);
			context.write(new Text(input[0]), new Text(input[1] + "," + input[2]));
		}
	}
	
	public static class CountReducerII extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String joinedOutput = "";
			for (Text val : values) {
				if (joinedOutput == "") {
					joinedOutput = joinedOutput + val.toString();
				} else {
					joinedOutput = joinedOutput + " " + val.toString();
				}
			}
			context.write(word, new Text(joinedOutput));
			
		}
	}
	
	public static class WordKey implements Writable, WritableComparable<WordKey> {
	    private Text word = null;
	    private IntWritable movieID = null;
	    
	    public WordKey() {
	    	word = new Text();
	    	movieID = new IntWritable();
	    }
	    
	    public WordKey(Text m, IntWritable o) {
	    	this.word = m;
	    	this.movieID = o;
	    }

	    @Override
	    public int compareTo(WordKey wordKey) {
	        int compareValue = this.word.compareTo(wordKey.getWord());
	        if(compareValue == 0 ){
	            compareValue = this.movieID.compareTo(wordKey.getMovieID());
	        }
	        return compareValue;
	    }
	    
	    public Text getWord() {
	    	return this.word;
	    }
	    
	    public IntWritable getMovieID() {
	    	return this.movieID;
	    }
	    
	    public void write(DataOutput out) throws IOException {
	    	word.write(out);
	    	movieID.write(out);
	    }
	    
	    public void readFields(DataInput in) throws IOException {
	    	word.clear();
	    	word.readFields(in);
	    	movieID.readFields(in);
	    }
	 }
	
	public static class WordJoiningGroupingComparator extends WritableComparator {
	    public WordJoiningGroupingComparator() {
	        super(WordKey.class, true);
	    }

	    @SuppressWarnings("rawtypes")
		@Override
	    public int compare(WritableComparable a, WritableComparable b) {
	    	WordKey movieKey1 = (WordKey)a;
	    	WordKey movieKey2 = (WordKey)b;
	        return movieKey1.compareTo(movieKey2);
	    }
	}
	
	public class WordJoiningPartitioner extends Partitioner<WordKey,Text> {
	    @Override
	    public int getPartition(WordKey movieID, Text text, int numPartitions) {
	        return movieID.getWord().hashCode() % numPartitions;
	    }
	}
	
	public static void main(String[] args) throws Exception {

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Index.class);
		job.setMapOutputKeyClass(WordKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}
		//job.addCacheFile(new URI(args[0]+"data/stop.txt#stopw"));

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setJarByClass(Index.class);
		job.setNumReduceTasks(1);
		job.setGroupingComparatorClass(WordJoiningGroupingComparator.class);
		job.setPartitionerClass(WordJoiningPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileSystem fs = FileSystem.get(conf);
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+"temp_index");
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileOutputFormat.setOutputPath(job, outputDestination);
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;

		
		int jobCompletionStatus2 = 99;
		
		if(jobCompletionStatus == 0){
			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2);
			job2.setJarByClass(Index.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			if (args.length > 2){
				job2.setNumReduceTasks(Integer.parseInt(args[2]));
			}
	
			job2.setMapperClass(CountMapperII.class);
			job2.setReducerClass(CountReducerII.class);
			//job2.setJarByClass(WordFreq.class);
			//job2.setSortComparatorClass(customComp.class);
			job2.setNumReduceTasks(1);
	
			FileInputFormat.addInputPath(job2, new Path(args[0]+"temp_index/part-r-00000"));
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