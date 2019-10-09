//	By: Tanushri Singh
//	CS 6350 - Big Data Management and Analytics
// 	Instructor: Latifur Khan

/*
WHAT IT SHOULD DO:-
Please use in-memory join to answer this question.
Given any two Users (they are friend) as input, output the list of the names and the city of their mutual friends.
Note: use the userdata.txt to get the extra user information. Output format:
UserA id, UserB id, list of [city] of their mutual Friends.
Sample Output:
0, 41 [Evangeline: Loveland, Agnes: Marietta]
*/

//Libraries
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;

	public class HW1_3 {
		public static class Map extends Mapper<LongWritable, Text, Text, Text>{
			private IntWritable[] frn = new IntWritable[2];
			private Text friendListInfo = new Text(); // type of output key
			static HashMap<String, String> dets;
			public Path path = new Path("hdfs://localhost:9000" +"/Users/tanushrisingh/Desktop/Spring 2019/Big Data Management/Assignment 1/userdata.txt");

			public void setup(Context context) throws IOException, InterruptedException {
				Configuration config = context.getConfiguration();
				dets = new HashMap<String, String>();
				FileSystem file = FileSystem.get(config);
				BufferedReader buffread = new BufferedReader(new InputStreamReader(file.open(path)));

				String userinfo;
				userinfo = buffread.readLine();
		       while(userinfo != null)
		       {
		    	   String[] s = userinfo.split(",");     //Split on a ,
		    	   String friendinfo = s[1] + ":" + s[4];
		    	   dets.put(s[0].trim(), friendinfo);
		    	   userinfo = buffread.readLine();
 		       }
		       buffread.close();
		    }

			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] line = value.toString().split("\\s+", -1);
				if (line.length < 2 || line[0].trim().isEmpty() || line[1].trim().isEmpty()) {
	            	return;
	            }
				String fr1 = line[0].trim();
				String[] friendLister = line[1].trim().split(",");
				int[] friendListerVal = Arrays.stream(friendLister).mapToInt(Integer::parseInt).toArray();
	            Arrays.sort(friendListerVal);
				String holder = "";

				String friendKey = "";
	            String fList = "";
	            for (int i = 0; i < friendListerVal.length; i++) {
	            	int var = friendListerVal[i];
	            	friendKey = Integer.toString(var);
	            	fList +=  friendKey+":"+dets.get(friendKey);
	            	if (i != friendListerVal.length-1) {
	            		fList += ",";
	            	}
	            }
	            friendListInfo.set(fList);
				for( String friends : friendLister){
					if(friends.isEmpty())
						continue;

					if (Integer.parseInt(fr1) > Integer.parseInt(friends)) {
						holder = friends+","+ fr1;
					}
					else {
						holder = fr1 + "," +friends;
					}
					Text contributors = new Text(holder);
					context.write(contributors, friendListInfo);
				}
			}
		}

		public static class Reduce extends Reducer<Text,Text,Text,Text> {
			public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
				String[] groupedList = new String[2];
	        	String[][] friendList = new String[2][];
	        	ArrayList<String> resultList = new ArrayList<>();
	        	String result = "[";
	        	int index = 0, var1 = 0, var2 = 0, var3=0;
	        	for (Text val : values) {
	        		groupedList[index] = val.toString();
	        		friendList[index] = groupedList[index].split(",");
	        		index++;
	        	}

            // Assembly of friends prior to writing them out
	        	String string1, string2;
	        	while (index == 2 && var1 < friendList[0].length && var2 < friendList[1].length) {
	        		string1 = friendList[0][var1];
	        		string2 = friendList[1][var2];
	        		String []array1 = string1.split(":");
	        		String []array2 = string2.split(":");
	        		if (array1[0].equals(array2[0])) {
	        			if (var3 != 0) {
	        				result += ",";
	        			}
	        			result += array2[1]+":"+array2[2];
	        			var3++;
	        			var1++;
	        			var2++;
	        		} else if (Integer.parseInt(array1[0]) > Integer.parseInt(array2[0])) {
	        			var2++;
	        		} else {
	        			var1++;
	        		}
	        	}
	        	result += "]";
	        	context.write(key, new Text(result));   // Final Write Out
		}
	}

		// Main Func:- Driver program
	  	public static void main(String[] args) throws Exception {
		  	Configuration config= new Configuration();
		  	String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		  	// getting arguments from commandline
		  	if (otherArgs.length != 2) {
			  	System.err.println("Usage: WordCount <in> <out>");
			  	System.exit(2);
		  	}

		  	// create a job
		  	Job job = new Job(config, "WordCount");
		  	job.setJarByClass(InMemoryJoin.class);                              // In order to select InMemoryJoin
		  	job.setMapperClass(Map.class);                                      // Set mapper class
		  	job.setReducerClass(Reduce.class);                                  // Set Reducer
		  	job.setMapOutputKeyClass(Text.class);                               // In order to set OutputKeyClass
		  	job.setMapOutputValueClass(Text.class);                             // In order to set map output value class type
		  	job.setOutputKeyClass(Text.class);                                  // In order to set the output keytype
		  	job.setOutputValueClass(IntWritable.class);                         // In order to set the output valuetype
		  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));          // In order to set the HDFS path for the input datatype
		  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		  	// In order to set the HDFS path for the output itself
		  	System.exit(job.waitForCompletion(true) ? 0 : 1);                   //Exit once system has completed execution
	  	}
	}
