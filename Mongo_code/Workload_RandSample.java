import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class Workload_RandSample {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
		MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

		conf.set("threshold", "0.2");
		Job job = Job.getInstance(conf, "Workload_RandSample");
		job.setJarByClass(Workload_RandSample.class);

		job.setMapperClass(Map.class);

		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Integer.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<Integer, BasicDBObject, Integer, BSONWritable> {
		private static double threshold;
		private Random rand = new Random();

		public void map(Integer key, BasicDBObject value, Context context) throws IOException, InterruptedException {

			BasicBSONObject output = new BasicBSONObject();
			BSONWritable reduceResult = new BSONWritable();

			Configuration conf = context.getConfiguration();
			String param = conf.get("threshold");
			threshold = Double.parseDouble(param);
			if (this.rand.nextDouble() < threshold) {

				output.put("value", value.getString("value"));
				reduceResult.setDoc(output);

				context.write(key, reduceResult);
			}
		}
	}

}

