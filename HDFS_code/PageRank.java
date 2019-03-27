import java.io.IOException;

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

public class PageRank {

	private static final double DAMPING_FACTOR = 0.85;
	private static final String SEPARATOR = "\t";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//first job for data pretreatment
		Job pretreatmentJob = pretreatmentJobSetting(conf, args, "", ".0");
		pretreatmentJob.waitForCompletion(true);

		//second job for updating page rank
		int i = 0;
		Job rankingUpdateJob;
		for (i = 1; i <= 1; i++) {
			rankingUpdateJob = rankingUpdateJobSetting(conf, args, "." + (i - 1), "." + i);
			rankingUpdateJob.waitForCompletion(true);
		}

		//third(last) job for cleaning up the result to show 'page' and 'rank' only.
		Job cleaningUpJob = cleaningUpJobSetting(conf, args, "." + (i - 1), ".out");
		System.exit(cleaningUpJob.waitForCompletion(true) ? 0 : 1);

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
				int numOfTo = parts.length - 1;
				if (numOfTo == 0)
					return;

				double rank = Double.parseDouble(parts[0]) / (double) numOfTo;
				for (int i = 1; i <= numOfTo; i++) {
					String to = parts[i];
					Text mapKey = new Text(to);
					Text mapValue = new Text(String.valueOf(rank).trim());
					context.write(mapKey, mapValue);
				}
			}
		}
	}

	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double totalRank = 0;
			String rankAndSetOfTo = null;

			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString().trim(), SEPARATOR);

				if (parts.length < 2) { // rank value
					totalRank += Double.parseDouble(parts[0]);
				} else { // original input data
					rankAndSetOfTo = value.toString();
				}
			}

			// This node doesn't have any inbound links.
			if (rankAndSetOfTo == null) {
				return;
			}

			double dampingFactor = (1d - DAMPING_FACTOR);
			double newRank = dampingFactor + (DAMPING_FACTOR) * (totalRank);

			//Find first index of SEPARATOR
			int index = rankAndSetOfTo.indexOf(SEPARATOR);
			//replace the old rank value before SEPARATOR to the new rank
			rankAndSetOfTo = newRank+rankAndSetOfTo.substring(index);

			context.write(key, new Text(rankAndSetOfTo));
		}
	}

	private static class CleaningUpMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String rankAndTo = value.toString();
			int index = rankAndTo.indexOf(SEPARATOR);
			String rank = rankAndTo.substring(0,index);
			context.write(key, new Text(rank));
		}
	}

	private static Job pretreatmentJobSetting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "pretreatment");
		job.setJarByClass(PageRank.class);

		job.setMapperClass(PretreatmentMap.class);
		job.setReducerClass(PretreatmentReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0] + from));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));
		return job;
	}

	private static Job rankingUpdateJobSetting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "updating page rank");
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

	private static Job cleaningUpJobSetting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "cleaning up");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(CleaningUpMap.class);

		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0] + from));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));

		return job;
	}

}
