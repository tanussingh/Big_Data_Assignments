//	By: Tanushri Singh
//	CS 6350 - Big Data Management and Analytics
// 	Instructor: Latifur Khan

/*
WHAT THIS SHOULD DO:-
Please answer this question by using dataset from Q1.
Find friend pairs whose number of common friends (number of mutual friend) is within the top-10 in all the pairs. Please
output them in decreasing order.
Output Format:
<User_A>, <User_B><TAB><Number of Mutual Friends><TAB><Mutual/Common Friend Number>
*/

//Libraries
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenFriends {
	public static class TopTenForMap1 extends Mapper<LongWritable, Text, Text, Text> {
		private Text orderFriend = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				String friend1 = line[0];
				List<String> values = Arrays.asList(line[1].split(","));
				for (String friend2 : values) {
					int fr1 = Integer.parseInt(friend1);
					int fr2 = Integer.parseInt(friend2);
					if (fr1 < fr2)
						orderFriend.set(friend1 + "," + friend2);
					else
						orderFriend.set(friend2 + "," + friend1);
					context.write(orderFriend, new Text(line[1]));
				}
			}
		}
	}

	public static class TopTenReduce1 extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			int mutualFriendCounter = 0;
			for (Text friends : values) {
				List<String> temporary = Arrays.asList(friends.toString().split(","));
				for (String friend : temporary) {
					if (map.containsKey(friend))
						mutualFriendCounter += 1;
					else
						map.put(friend, 1);
				}
			}
			context.write(key, new IntWritable(mutualFriendCounter));
		}
	}

	public static class TopTenForMap2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				context.write(one, value);
			}
		}
	}

	public static class TopTenReduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {
			public  void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			int count = 1;
			for (Text line : values) {
				String[] fields = line.toString().split("\t");
				if (fields.length == 2) {
					map.put(fields[0], Integer.parseInt(fields[1]));
				}
			}
			compareValues updatedValues = new compareValues(map);
			TreeMap<String, Integer> sortedMap = new TreeMap<String,Integer>(updatedValues);
			sortedMap.putAll(map);

			for (Entry<String, Integer> entry : sortedMap.entrySet()) {
					if (count <= 10) {
						context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));

					}
					else
						break;
					count++;
				}
			}
		}

//Defining compareValues explicitly so that it can be sorted in Descending Order on Map
	public static class compareValues implements Comparator<String> {
		HashMap<String, Integer> value;
		public compareValues(HashMap<String, Integer> value) {
			this.value = value;
		}
		public int compare(String string1, String string2) {
			if (value.get(string1) >= value.get(string2)) {return -1;}
			else {return 1;}
		}
	}

	//Main Func:- Driver Function
	public static void main(String[] args) throws Exception{
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		//Getting all arguments from command line
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopTepFriends <inputfile HDFS path> <outputfile1 HDFS path> <outputfile2 HDFS path>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job jobForPhase1 = new Job(config, "TopTepFriends, PHASE 1");
		jobforPhase1.setJarByClass(TopTenFriends.class);
		jobforPhase1.setMapperClass(TopTenForMap1.class);
		jobforPhase1.setReducerClass(TopTenReduce1.class);
		jobforPhase1.setMapOutputKeyClass(Text.class);
		jobforPhase1.setMapOutputValueClass(Text.class);
		jobforPhase1.setOutputKeyClass(Text.class);
		jobforPhase1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));				// setthing HDFS path for output1 (output has count of mutual friends between 2 friends)
		boolean mapreduce = jobforPhase1.waitForCompletion(true);

		if (mapreduce) {
			Configuration config1 = new Configuration();
			@SuppressWarnings("deprecation")
			Job jobForPhase2 = new Job(config1, "TopTenFriends, PHASE 2");
			jobForPhase2.setJarByClass(TopTenFriends.class);												// Setting setJarByClass
			jobForPhase2.setMapperClass(TopTenForMap2.class);													// Setting MapperClass
			jobForPhase2.setReducerClass(TopTenReduce2.class);											// Setting Reducer
			jobForPhase2.setInputFormatClass(TextInputFormat.class);								// Setting Input Class
			jobForPhase2.setMapOutputKeyClass(IntWritable.class);										// Setting Map Outputkey
			jobForPhase2.setMapOutputValueClass(Text.class);												// Setting Output Value Class for map
			jobForPhase2.setOutputKeyClass(Text.class);															// setting output keytype
			jobForPhase2.setOutputValueClass(IntWritable.class);										// setting output valuetype
			FileInputFormat.addInputPath(jobForPhase2, new Path(otherArgs[1]));			// send output of MapReduce phase1 as input to MapReduce phase2
			FileOutputFormat.setOutputPath(jobForPhase2, new Path(otherArgs[2]));		// set the HDFS path for the output2
			System.exit(jobForPhase2.waitForCompletion(true) ? 0 : 1);							// Quit once execution completes
		}
	}
}
