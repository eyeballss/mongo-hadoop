import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Workload_PageRank {

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value); // original input data

			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			if (parts.length > 2) {
				double outboundPageRank = Double.parseDouble(parts[1]) / (parts.length - 2);
				for (int i = 2; i < parts.length; i++) {
					String outbound = parts[i];
					outKey.set(outbound);
					outValue.set(String.valueOf(outboundPageRank));
					context.write(outKey, outValue);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public static final double DAMPING_FACTOR = 0.85;

		private Text outValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double totalPageRank = 0;
			int allCount = 0;
			ArrayList<String> outbounds = null;

			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

				if (parts.length < 2) { // rank value
					totalPageRank += Double.parseDouble(parts[0]);
				} else { // original input data
					outbounds = new ArrayList<String>();
					allCount = Integer.parseInt(parts[0]);
					for (int i = 2; i < parts.length; i++) {
						outbounds.add(parts[i]);
					}
				}
			}

			// this node doesn't have any inbound links.
			if (outbounds == null) {
				return;
			}

			double dampingFactor = ((1d - DAMPING_FACTOR) / (double) allCount);
			double newPageRank = dampingFactor + DAMPING_FACTOR * (totalPageRank);

			StringBuilder sb = new StringBuilder();
			sb.append(allCount + "\t" + newPageRank);
			for (String outbound : outbounds) {
				sb.append("\t" + outbound);
			}

			context.write(key, new Text(sb.toString()));

		}
	}

	private static class FinishingMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String values[] = line.split("\t");

			context.write(key, new Text(values[1]));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		int i = 0;
		for (i = 1; i <= 2; i++) {
			String from = "." + (i - 1);
			if (i == 1)
				from = "";
			String to = "." + i;

			Job job = Job.getInstance(conf);
			job.setJarByClass(Workload_PageRank.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0] + from));
			FileOutputFormat.setOutputPath(job, new Path(args[0] + to));

			job.waitForCompletion(true);

		}

		Job job2 = Job.getInstance(conf, "PageRankFinishing");
		job2.setJarByClass(Workload_PageRank.class);
		job2.setNumReduceTasks(0);
		job2.setMapperClass(FinishingMap.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[0] + "." + (i - 1)));
		FileOutputFormat.setOutputPath(job2, new Path(args[0] + ".out"));

		job2.waitForCompletion(true);
	}
}

