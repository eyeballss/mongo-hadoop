import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.splitter.ShardMongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class RandSample {
	private static final String THRESHOLD_NAME = "threshold";
	private static final String THRESHOLD_VALUE = "0.2";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(THRESHOLD_NAME, THRESHOLD_VALUE);

		MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
		MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[0] + ".out");
		MongoConfigUtil.setSplitterClass(conf, ShardMongoSplitter.class);

		Job job = Job.getInstance(conf, "RandSample");
		job.setJarByClass(RandSample.class);

		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Integer.class);
		job.setMapOutputValueClass(BSONWritable.class);

		job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<Integer, BasicDBObject, Integer, BSONWritable> {
		static double threshold;
		private Random rand;
		private BasicBSONObject output;
		private BSONWritable reduceResult;

		@Override
		protected void setup(Mapper<Integer, BasicDBObject, Integer, BSONWritable>.Context context)
				throws IOException, InterruptedException {
			rand = new Random();
			Configuration conf = context.getConfiguration();
			String param = conf.get(THRESHOLD_NAME);
			threshold = Double.parseDouble(param);

			output = new BasicBSONObject();
			reduceResult = new BSONWritable();
		}

		public void map(Integer key, BasicDBObject value, Context context) throws IOException, InterruptedException {
			if (this.rand.nextDouble() < threshold) {
				output.put("value", value.getString("data"));
				reduceResult.setDoc(output);
				context.write(key, reduceResult);
			}
		}
	}
}
