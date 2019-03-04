import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

	private static final Text STATIC_KEY = new Text("statickKey");
	private static final String ALL_COUNT = "allCount";
	private static final double DAMPING_FACTOR = 0.85;
	private static final String SEPARATOR = "\t";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = job1Setting(conf, args, "", ".all.count");
		job.waitForCompletion(true);

		try {
			Path pt = new Path("hdfs:" + args[0] + ".all.count/part-r-00000");// Location of file in HDFS
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while ((line = br.readLine()) != null) {
				conf.set(ALL_COUNT, line);
			}
		} catch (Exception e) {
		}

		Job job2 = job2Setting(conf, args, "", ".0");
		job2.waitForCompletion(true);

		int i = 0;
		Job job3;
		for (i = 1; i <= 1; i++) {
			job3 = jobRankSetting(conf, args, "." + (i - 1), "." + i);
			job3.waitForCompletion(true);
		}

		Job job4 = jobCleanUpSetting(conf, args, "." + (i - 1), ".out");
		job4.waitForCompletion(true);

	}

	public static class AllNodesCountMap extends Mapper<Text, Text, Text, Text> {
		HashSet<String> set = new HashSet<>();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			set.add(key.toString());
//			set.add(value.toString());
		}

		protected void cleanup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<String> itr = set.iterator();
			while (itr.hasNext()) {
				context.write(new Text(STATIC_KEY), new Text(itr.next()));
			}
		}

	}

	public static class AllNodesReduce extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<>();
			for (Text temp : values) {
				String value = temp.toString();
				set.add(value);
			}
			context.write(NullWritable.get(), new Text(String.valueOf(set.size())));
		}
	}

	public static class PretreatmentMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class PretreatmentReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			String allCount = context.getConfiguration().get(ALL_COUNT);
			double initRank = 1d / (Double.parseDouble(allCount));
			sb.append(initRank);
			for (Text value : values) {
				sb.append(SEPARATOR);
				sb.append(value.toString());
			}

			context.write(key, new Text(sb.toString()));
		}
	}

	public static class PageRankMap extends Mapper<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value); // original input data

			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), SEPARATOR);
			if (parts.length > 2) {
				double outboundPageRank = Double.parseDouble(parts[0]) / (parts.length - 1);
				for (int i = 1; i < parts.length; i++) {
					String outbound = parts[i];
					outKey.set(outbound);
					outValue.set(String.valueOf(outboundPageRank));
					context.write(outKey, outValue);
				}
			}
		}
	}

	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double totalPageRank = 0;
			int allCount = 0;
			ArrayList<String> outbounds = null;

			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), SEPARATOR);

				if (parts.length < 2) { // rank value
					totalPageRank += Double.parseDouble(parts[0]);
				} else { // original input data
					outbounds = new ArrayList<String>();
					allCount = Integer.parseInt(context.getConfiguration().get(ALL_COUNT));
					for (int i = 1; i < parts.length; i++) {
						outbounds.add(parts[i]);
					}
				}
			}

			// this node doesn't have any inbound links.
			if (outbounds == null) {
				return;
			}

			double dampingFactor = ((1d - DAMPING_FACTOR) / (double) allCount);
			double newPageRank = dampingFactor + (DAMPING_FACTOR) * (totalPageRank);

			StringBuilder sb = new StringBuilder();
			sb.append(newPageRank);
			for (String outbound : outbounds) {
				sb.append(SEPARATOR + outbound);
			}

			context.write(key, new Text(sb.toString()));

		}
	}

	private static class CleanUpMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String values[] = line.split(SEPARATOR);

			context.write(key, new Text(values[0]));
		}
	}

	private static Job job1Setting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, ALL_COUNT);
		job.setJarByClass(PageRank.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(AllNodesCountMap.class);
		job.setReducerClass(AllNodesReduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0] + from));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));
		return job;
	}

	private static Job job2Setting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "pretreatment");
		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PretreatmentMap.class);
		job.setReducerClass(PretreatmentReduce.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0] + from));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));
		return job;
	}

	private static Job jobRankSetting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "page rank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMap.class);
		job.setReducerClass(PageRankReduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0] + from));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));

		return job;
	}

	private static Job jobCleanUpSetting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "PageRankFinishing");
		job.setJarByClass(PageRank.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(CleanUpMap.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0] + from));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));

		return job;
	}

}

