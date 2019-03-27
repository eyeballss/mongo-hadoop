import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Workload_RandSample {
	private static final String THRESHOLD_NAME = "threshold";
	private static final String THRESHOLD_VALUE = "0.2";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(THRESHOLD_NAME, THRESHOLD_VALUE);

		Job job = Job.getInstance(conf, "Workload_RandSample");
		job.setJarByClass(Workload_RandSample.class);

		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + ".out"));

		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<Object, Text, NullWritable, Text> {
		static double threshold;
		private Random rand;

		@Override
		protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			rand = new Random();
			Configuration conf = context.getConfiguration();
			String param = conf.get(THRESHOLD_NAME);
			threshold = Double.parseDouble(param);
		}

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			if (this.rand.nextDouble() < threshold) {
				context.write(NullWritable.get(), value);
			}
		}
	}
}
