import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

	private static final double DAMPING_FACTOR = 0.85;
	private static final String SEPARATOR = "\t";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

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

	public static class PretreatmentMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class PretreatmentReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			double initRank = 1d;
			sb.append(initRank);
			for (Text value : values) {
				sb.append(SEPARATOR);
				sb.append(value.toString());
			}

			context.write(key, new Text(sb.toString()));
		}
	}

	public static class PageRankMap extends Mapper<Text, Text, Text, Text> {

		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value); // original input data

			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), SEPARATOR);
			if (parts.length >= 2) {
				int qnsah = parts.length - 1;
				if (qnsah == 0)
					return;

				double outboundPageRank = Double.parseDouble(parts[0]) / (double) qnsah;
				for (int i = 1; i <= qnsah; i++) {
					String outbound = parts[i];
					Text outKey = new Text(outbound);
					Text outValue = new Text(String.valueOf(outboundPageRank).trim());
					context.write(outKey, outValue);
				}
			}
		}
	}

	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double totalPageRank = 0;
			String outbounds = null;

			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString().trim(), SEPARATOR);

				if (parts.length < 2) { // rank value
					totalPageRank += Double.parseDouble(parts[0]);
				} else { // original input data
					outbounds = value.toString();
				}
			}

			// this node doesn't have any inbound links.
			if (outbounds == null) {
				return;
			}

			double dampingFactor = (1d - DAMPING_FACTOR);
			double newPageRank = dampingFactor + (DAMPING_FACTOR) * (totalPageRank);

			String[] result = outbounds.split(SEPARATOR);

			StringBuilder sb = new StringBuilder();
			sb.append(newPageRank);
			for (int i = 1; i < result.length; i++) {
				sb.append(SEPARATOR + result[i]);
			}

			context.write(key, new Text(sb.toString()));
			totalPageRank = 0;
		}
	}

	private static class CleanUpMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String values[] = line.split(SEPARATOR);

			context.write(key, new Text(values[0]));
		}
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
