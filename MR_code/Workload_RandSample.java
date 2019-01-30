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
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("threshold", "0.2");
		Job job = Job.getInstance(conf, "Workload_RandSample");
		job.setJarByClass(Workload_RandSample.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<Object, Text, NullWritable, Text> {
		static double threshold;
		private Random rand = new Random();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String param = conf.get("threshold");
			threshold = Double.parseDouble(param);
			if (this.rand.nextDouble() < threshold) {
				context.write(NullWritable.get(), value);
			}
		}
	}

}

