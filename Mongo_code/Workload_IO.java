import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class Workload_IO {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
		MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

		Job job = Job.getInstance(conf, "Worklaod_IO");
		job.setJarByClass(Workload_IO.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(BSONWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		
		job.waitForCompletion(true);

	}

	public static class Map extends Mapper<Integer, BasicDBObject, IntWritable, Text> {
		public void map(Integer key, BasicDBObject value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(key), new Text(value.getString("data")));
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, Integer, BSONWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			BasicBSONObject output = new BasicBSONObject();
			BSONWritable reduceResult = new BSONWritable();

			Text val = values.iterator().next();
			if (val != null) {
				output.put("value", val.toString());
				reduceResult.setDoc(output);
				context.write(key.get(), reduceResult);
			}
		}
	}
}

