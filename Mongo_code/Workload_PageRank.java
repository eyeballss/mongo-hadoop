import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/*

data format

{ "_id" : 1, "from" : 0, "to" : 3 }
{ "_id" : 2, "from" : 1, "to" : 2 }
{ "_id" : 3, "from" : 0, "to" : 2 }
{ "_id" : 9, "from" : 2, "to" : 6 }
{ "_id" : 6, "from" : 4, "to" : 5 }
{ "_id" : 7, "from" : 4, "to" : 2 }
{ "_id" : 4, "from" : 0, "to" : 5 }
{ "_id" : 5, "from" : 5, "to" : 0 }
{ "_id" : 8, "from" : 3, "to" : 7 }

 */

public class Workload_PageRank {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
		MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

		Job job = Job.getInstance(conf, "PageRankPreparing");
		job.setJarByClass(Workload_PageRank.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(PreparingMap.class);
		job.setReducerClass(PreparingReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0] + "-pr" + 0));
		job.setInputFormatClass(MongoInputFormat.class);

		job.waitForCompletion(true);

		int i = 1;
		for (i = 1; i <= 2; i++) {
			Job job2 = Job.getInstance(conf, "PageRank");
			job2.setJarByClass(Workload_PageRank.class);
			job2.setMapperClass(PageRankMap.class);
			job2.setReducerClass(PageRankReduce.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(args[0] + "-pr" + (i - 1)));
			FileOutputFormat.setOutputPath(job2, new Path(args[0] + "-pr" + i));
			job2.waitForCompletion(true);
		}

		Job job3 = Job.getInstance(conf, "PageRankFinishing");
		job3.setJarByClass(Workload_PageRank.class);

		job3.setNumReduceTasks(0);

		job3.setMapperClass(FinishingMap.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(BSONWritable.class);
		FileInputFormat.addInputPath(job3, new Path(args[0] + "-pr" + (i - 1)));
		job3.setOutputFormatClass(MongoOutputFormat.class);
		job3.waitForCompletion(true);
	}

	private static class PreparingMap extends Mapper<Integer, BasicDBObject, Text, Text> {
		public void map(Integer key, BasicDBObject value, Context context) throws IOException, InterruptedException {
			String from = value.getString("from");
			String to = value.getString("to");

			context.write(new Text(from), new Text(to));
			context.write(new Text("-1"), new Text(from));
			context.write(new Text("-1"), new Text(to));

		}
	}

	private static class PreparingReduce extends Reducer<Text, Text, Text, NullWritable> {
		private static int allCount = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			if (key.toString().equals("-1")) {
				HashSet<String> set = new HashSet<>();
				for (Text value : values) {
					set.add(value.toString());
				}
				allCount = set.size();
			} else {
				if (allCount == 0)
					return;
				String val = key.toString() + "\t" + ((double) 1.0 / (double) allCount);
				for (Text value : values) {
					val += "\t" + value.toString();
				}
				context.write(new Text(val), NullWritable.get());
			}
		}
	}

	private static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String values[] = line.split("\t");

			if (values == null || values.length < 3)
				return;

			String cur_node = values[0];
			double cur_pr = Double.parseDouble(values[1]);
			int cnt_out_edge = values.length - 2;

			String buf = "";
			int leng = values.length;
			for (int i = 2; i < leng; i++) {
				context.write(new Text(values[i]), new Text(String.valueOf((cur_pr / (double) cnt_out_edge))));
				if (buf.length() != 0)
					buf += "\t";
				buf += values[i];
			}
			context.write(new Text(cur_node), new Text("*" + buf));
		}
	}

	private static class PageRankReduce extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double init_value = 1d / 5000d; // needs to fix here

			String out_edges = "";
			double cur_pr = 0.0d;

			for (Text temp : values) {
				String value = temp.toString();
				if (value == null || value.length() == 0)
					return;

				if (value.startsWith("*")) {
					out_edges = value;
					continue;
				}

				cur_pr += Double.parseDouble(value);
			}

			cur_pr *= 0.85d;
			cur_pr += 0.15d * init_value;

			if (out_edges.length() < 2)
				return;
			context.write(NullWritable.get(), new Text(key.toString() + "\t" + cur_pr + "\t" + out_edges.substring(1)));
		}

	}

	private static class FinishingMap extends Mapper<Object, Text, Text, BSONWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			BasicBSONObject output = new BasicBSONObject();
			BSONWritable reduceResult = new BSONWritable();

			String line = value.toString();
			String values[] = line.split("\t");

			output.put("rank", values[1].toString());
			reduceResult.setDoc(output);

			context.write(new Text(values[0]), reduceResult);
		}
	}
}

