//	By: Tanushri Singh
//	CS 6350 - Big Data Management and Analytics
// 	Instructor: Latifur Khan

/*
WHAT IT SHOULD DO:-
Using reduce-side join and job chaining:
Step 1: Calculate the average age of the direct friends of each user.
Step 2: Sort the users by the average age from step 1 in descending order.
Step 3. Output the tail 15 (15 lowest averages) users from step 2 with their address and the
calculated average age.
Sample output:
User A, 1000 Anderson blvd, Dallas, TX, average age of direct friends.
*/

//Libraries
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class HW1_4 {
	static int i = 0;

	public static class mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		LongWritable userVal = new LongWritable();
		Text friend = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			userVal.set(Long.parseLong(line[0]));
			Text data = new Text();
			data.set(userVal.toString());

			if (line.length != 1) {
				String mutualFriends = line[1];
				String outvalue = ("U:" + mutualFriends.toString());
				context.write(userVal, new Text(outvalue));
			}
		}
	}

	public static class firstMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable outkey = new LongWritable();
		private Text outvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			if (arr.length == 10) {

				outkey.set(Long.parseLong(arr[0]));
				String[] cal = arr[9].toString().split("/");
				System.out.println("Ratings");
				Date current = new Date();
				int currentMonth = current.getMonth() + 1;
				int currentYear = current.getYear() + 1900;
				int result = currentYear - Integer.parseInt(cal[2]);

				if (Integer.parseInt(cal[0]) > currentMonth) {
					result--;
				} else if (Integer.parseInt(cal[0]) == currentMonth) {
					int currentDay = current.getDate();

					if (Integer.parseInt(cal[1]) > currentDay) {
						result--;
					}
				}
				String data = arr[1] + "," + new Integer(result).toString() + "," + arr[3] + "," + arr[4] + "," + arr[5];
				outvalue.set("R:" + data);
				context.write(outkey, outvalue);
			}
		}
	}

	public static class firstReducer extends Reducer<LongWritable, Text, Text, Text> {
		private ArrayList<Text> partialListA = new ArrayList<Text>();
		private ArrayList<Text> partialListB = new ArrayList<Text>();
		HashMap<String, String> mapProduced = new HashMap<>();

		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			mapProduced = new HashMap<String, String>();
			String mydataOfInterestPath = config.get("dataOfInterest");

			Path givenPath = new Path("hdfs://cshadoop1" + mydataOfInterestPath);
			FileSystem fs = FileSystem.get(config);
			BufferedReader buffRead = new BufferedReader(new InputStreamReader(fs.open(givenPath)));
			String line;
			line = buffRead.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					mapProduced.put(arr[0].trim(), arr[1] + ":" + arr[3] + ":" + arr[9]);
				}
				line = buffRead.readLine();
			}
		}

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			partialListA.clear();
			partialListB.clear();

			for (Text value : values) {
				if (value.toString().charAt(0) == 'U') {
					partialListA.add(new Text(value.toString().substring(2)));
				} else if (value.toString().charAt(0) == 'R') {
					partialListB.add(new Text(value.toString().substring(2)));
				}
			}

			Text critical = new Text();
			float age = 0;
			int counter = 0;
			float avgAge;
			String[] specifics = null;

			if (!partialListA.isEmpty() && !partialListB.isEmpty()) {
				for (Text text1 : partialListA) {
					String friend[] = text1.toString().split(",");

					for (int i = 0; i < friend.length; i++) {
						if (mapProduced.containsKey(friend[i])) {
							String[] ageCalu = mapProduced.get(friend[i]).split(":");
							Date current = new Date();
							int Month = current.getMonth() + 1;
							int Year = current.getYear() + 1900;
							String[] cal = ageCalu[2].toString().split("/");
							int result = Year - Integer.parseInt(cal[2]);

							if (Integer.parseInt(cal[0]) > Month) {
								result--;
							} else if (Integer.parseInt(cal[0]) == Month) {
								int Day = current.getDate();

								if (Integer.parseInt(cal[1]) > Day) {
									result--;
								}
							}
							age += result;
							counter++;
						}
					}
					avgAge = (float) (age / counter);
					String S = "";

					for (Text text2 : partialListB) {
						specifics = text2.toString().split(",");
						S = text2.toString() + "," + new Text(new FloatWritable((float) avgAge).toString());
					}

					critical.set(S);
				}
			}
			context.write(new Text(key.toString()), critical);
		}

	}

	public static class secondMapper extends Mapper<LongWritable, Text, UserPageWritable, Text> {
		private Long outkey = new Long(0L);
		public Long getOutkey() {
			return outkey;
		}

		public void setOutkey(Long outkey) {
			this.outkey = outkey;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] m = value.toString().split("\t");
			Long l = Long.parseLong(m[0]);
			outkey = l;
			if (m.length == 2) {
				String line[] = m[1].split(",");
				context.write(new UserPageWritable(Float.parseFloat(m[0]), Float.parseFloat(line[5])), new Text(m[1].toString()));
			}
		}
	}

	public static class UserPageWritable implements WritableComparable<UserPageWritable> {
		private Float idOfUser;
		private Float friendId;
		public Float getidOfUser() {
			return idOfUser;
		}

		public void setidOfUser(Float idOfUser) {
			this.idOfUser = idOfUser;
		}

		public Float getFriendId() {
			return friendId;
		}

		public void setFriendId(Float friendId) {
			this.friendId = friendId;
		}

		public UserPageWritable(Float user, Float friend1) {
			// TODO Auto-generated constructor stub
			this.idOfUser = user;
			this.friendId = friend1;
		}

		public UserPageWritable() {
		}

		//Override the following fields
		@Override
		public void readFields(DataInput i) throws IOException {
			idOfUser = i.readFloat();
			friendId = i.readFloat();
		}

		@Override
		public void write(DataOutput o) throws IOException {
			o.writeFloat(idOfUser);
			;
			o.writeFloat(friendId);
			;
		}

		@Override
		public int compareTo(UserPageWritable o) {
			// TODO Auto-generated method stub

			int result = idOfUser.compareTo(o.idOfUser);
			if (result != 0) { return result; }
			return this.friendId.compareTo(o.friendId);
		}

		@Override
		public String toString() {
			return idOfUser.toString() + ":" + friendId.toString();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) { return false; }
			if (getClass() != obj.getClass()) { return false; }
			final UserPageWritable other = (UserPageWritable) obj;
			if (this.idOfUser != other.idOfUser && (this.idOfUser == null || !this.idOfUser.equals(other.idOfUser))) { return false; }
			if (this.friendId != other.friendId && (this.friendId == null || !this.friendId.equals(other.friendId))) { return false; }
			return true;
		}
	}

	public static class secondarySortComparator extends WritableComparator {
		public secondarySortComparator() {
			super(UserPageWritable.class, true);
		}

		@Override
		public int compare(WritableComparable write1, WritableComparable write2) {
			UserPageWritable key1 = (UserPageWritable) write1;
			UserPageWritable key2 = (UserPageWritable) write2;
			int results = -1 * key1.getFriendId().compareTo(key2.getFriendId());
			return results;
		}
	}

	public class tempPartitioner extends Partitioner<UserPageWritable, Text> {
		public int getPartition(UserPageWritable temperaturePair, Text nullWritable, int numPartitions) {
			return temperaturePair.getFriendId().hashCode() % numPartitions;
		}
	}

	public static class userRatingReducer extends Reducer<UserPageWritable, Text, Text, Text> {
		private ArrayList<Text> partialListA = new ArrayList<Text>();
		private ArrayList<Text> partialListB = new ArrayList<Text>();
		int i = 0;
		TreeMap<String, String> treeMap = new TreeMap<String, String>();

		public void reduce(UserPageWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text t : values) {
				if (treeMap.tailmap(15)) {
					treeMap.put(key.idOfUser.toString(), t.toString());
					context.write(new Text(t.toString().split(",")[0]), new Text(t));
				}
			}
		}
	}

	public static class secondarySortGrouping extends WritableComparator {
		public secondarySortGrouping() {
			super(UserPageWritable.class, true);
		}

		public int compare(WritableComparable write1, WritableComparable write2) {
			UserPageWritable key1 = (UserPageWritable) write1;
			UserPageWritable key2 = (UserPageWritable) write2;
			return -1 * key1.getFriendId().compareTo(key2.getFriendId());
		}
	}

	// Main Func:- Driver code
	public static void main(String[] args) throws Exception {
		Path outputDirectoryInt1 = new Path(args[3] + "_int1");
		Path outputDirectoryInt2 = new Path(args[4] + "_int2");

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("dataOfInterest", otherArgs[0]);

		// get arguments from command line
		if (otherArgs.length != 5) {
			System.err.println("Usage: JoinExample <in> <in2> <in3> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "JOIN 1 ");
		job.setJarByClass(HW1_4.class);
		job.setReducerClass(firstReducer.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, mapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, firstMapper.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, outputDirectoryInt1);

		int code = job.waitForCompletion(true) ? 0 : 1;
		Job job1 = new Job(new Configuration(), "JOIN 2");
		job1.setJarByClass(HW1_4.class);
		FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));

		job1.setMapOutputKeyClass(UserPageWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setPartitionerClass(tempPartitioner.class);
		job1.setMapperClass(secondMapper.class);]
		job1.setSortComparatorClass(secondarySortComparator.class);
		job1.setGroupingComparatorClass(secondarySortGrouping.class);
		job1.setReducerClass(userRatingReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job1, outputDirectoryInt2);

		// Execute job and grab exit code
		code = job1.waitForCompletion(true) ? 0 : 1;
	}
}
