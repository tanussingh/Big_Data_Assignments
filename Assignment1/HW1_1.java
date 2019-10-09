//	By: Tanushri Singh
//	CS 6350 - Big Data Management and Analytics
// 	Instructor: Latifur Khan

/*
WHAT IT SHOULD DO:-
Write a MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends". The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.
For example,
Alice’s friends are Bob, Sam, Sara, Nancy Bob’s friends are Alice, Sam, Clara, Nancy Sara’s friends are Alice, Sam, Clara, Nancy
As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output).

Output: The output should contain one line per user in the following format:
<User_A>, <User_B><TAB><Mutual/Common Friend List>
where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A and B are friend). < Mutual/Common Friend List > is a comma-separated list of unique IDs corresponding to mutual friend list of User A and B.
Please find the output for the following pairs:
(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)
*/

//Libaries
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class HW1_1 {

	//Map Function
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text friendList = new Text();					//Keeps track of all friends for user

		//Mapper Function
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t", -1);
			if (line.length < 2 || line[0].trim().isEmpty() || line[1].trim().isEmpty()) {
							return;
						}
			String friend1 = line[0].trim();
			String res = line[1].trim();
			String[] friendLister = line[1].trim().split(",");
			int[] friendListVal = Arrays.stream(friendLister).mapToInt(Integer::parseInt).toArray();
						Arrays.sort(friendListVal);
						String sortedFriendList = Arrays.stream(friendListVal).mapToObj(String::valueOf).collect(Collectors.joining(","));
						friendList.set(sortedFriendList);
			String stringValue = "";
			for( String friends : friendLister)
			{
				if(friends.isEmpty() || friend1.isEmpty())
					continue;
				if (Integer.parseInt(friend1) > Integer.parseInt(friends)) {
					stringValue = friends+","+ friend1;
				}
				else {
					stringValue = friend1 + ","+friends;
				}
				Text contributors = new Text(stringValue);
				context.write(contributors, friendList);
			}
		}
	}

	//Reducer Function
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		String residual = "No Mutual Friends";
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			ArrayList<String> mutual = new ArrayList<>(Arrays.asList(values.iterator().next().toString().split(",")));
			for(Text T: values){
				mutual.retainAll(new ArrayList<String>(Arrays.asList(T.toString().split(","))));
			}
			if(mutual.size() > 0){
				context.write(key, new Text(String.join(",", mutual)));
			}else{
				context.write(key, new Text(residual));
			}
	}
}

	// Main Func:- Driver program
		public static void main(String[] args) throws Exception {
			Configuration config = new Configuration();
			String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

			// get all args from command line. exit on error.
			if (otherArgs.length != 2) {
				System.err.println("Usage: WordCount <in> <out>");
				System.exit(2);
			}

			// create jobs
			Job job = new Job(config, "WordCount");
			job.setJarByClass(HW1_1.class);																	// Setting setJarByClass
			job.setMapperClass(Map.class);																	// Setting MapperClass
			job.setReducerClass(Reduce.class);															// Setting Reducer
			job.setMapOutputKeyClass(Text.class);														// Setting Map Outputkey
			job.setMapOutputValueClass(Text.class);													// Setting Output Value Class for map
			job.setOutputKeyClass(Text.class);															// setting output keytype
			job.setOutputValueClass(IntWritable.class);											// setting output valuetype
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));			// setting HDFS path of input data
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		// setting HDFS path for output
			System.exit(job.waitForCompletion(true) ? 0 : 1);								// Exit on completion
		}
}
