package assignment2;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Join {

	public static class MovieJoinMapper extends Mapper<Object, Text, MovieKey, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			if (line != null) {
				String[] output = null;
				output = line.split("\t");				
				MovieKey mk = new MovieKey(new Text(output[0]), new IntWritable(1));
				context.write(mk, new Text(output[2]));
			}
		}
	}

	public static class CharacterJoinMapper extends Mapper<Object, Text, MovieKey, Text>{
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			if (line != null) {
				String[] output = null;
				output = line.split("\t");				
				MovieKey mk = new MovieKey(new Text(output[0]), new IntWritable(2));
				context.write(mk, new Text(output[8]));
			}
		}
	}
	
	public static class MovieKey implements Writable, WritableComparable<MovieKey>{

		private Text movieID = null;
		private IntWritable rf = null;
		
		public Text getMovieID() {
			return movieID;
		}

		public void setMovieID(Text movieID) {
			this.movieID = movieID;
		}

		public IntWritable getRf() {
			return rf;
		}

		public void setRf(IntWritable rf) {
			this.rf = rf;
		}

		
		public MovieKey(){
			movieID = new Text();
			rf = new IntWritable();
		}
		
		
		public MovieKey(Text t, IntWritable iw){
			this.movieID = t;
			this.rf = iw;
		}
		
		@Override
		public int compareTo(MovieKey moviekey) {
			// TODO Auto-generated method stub
			int compVal = this.movieID.compareTo(moviekey.getMovieID());
			if(compVal == 0){
				compVal = this.rf.compareTo(moviekey.getRf());
			}
			return compVal;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			movieID.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			movieID.write(out);
		}
		
	}
	
	public static class CountReducer extends Reducer<MovieKey, Text, Text, Text> {
		
		public void reduce(MovieKey mk, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String joinedOutput = "";
			for (Text val : values) {
				if (joinedOutput == "") {
					joinedOutput = joinedOutput + val.toString();
				} else {
					joinedOutput = joinedOutput + "," + val.toString();
				}
			}
			context.write(mk.getMovieID(), new Text(joinedOutput));
		}
	}

	public class MovieJoiningPartitioner extends Partitioner<MovieKey,Text> {
	    @Override
	    public int getPartition(MovieKey movieID, Text text, int numPartitions) {
	        return movieID.getMovieID().hashCode() % numPartitions;
	    }
	}
	
	public static class MovieJoiningGroupingComparator extends WritableComparator {
	    public MovieJoiningGroupingComparator() {
	        super(MovieKey.class, true);
	    }

	    @SuppressWarnings("rawtypes")
		@Override
	    public int compare(WritableComparable a, WritableComparable b) {
	    	MovieKey movieKey1 = (MovieKey)a;
	    	MovieKey movieKey2 = (MovieKey)b;
	        return movieKey1.getMovieID().compareTo(movieKey2.getMovieID());
	    }
	}
	
	
	public static void main(String[] args) throws Exception {

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Join.class);
		job.setMapOutputKeyClass(MovieKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.setPartitionerClass(MovieJoiningPartitioner.class);
		job.setGroupingComparatorClass(MovieJoiningGroupingComparator.class);
		//job.setMapperClass(MovieJoinMapper.class);
		//job.setMapperClass(CharacterJoinMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setJarByClass(Join.class);
		job.setNumReduceTasks(1);
		

		
		// Use MultipleInputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical standpoint
		MultipleInputs.addInputPath(job, new Path(args[0]+"data/movie.metadata.tsv"), TextInputFormat.class, MovieJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[0]+"data/character.metadata.tsv"), TextInputFormat.class, CharacterJoinMapper.class);
		
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