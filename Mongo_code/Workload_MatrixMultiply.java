import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class Workload_MatrixMultiply {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
			System.exit(2);
		}
		Configuration conf = new Configuration();

    MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
    MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

		// M is an m-by-n matrix; N is an n-by-p matrix.
		conf.set("m", "1000");
		conf.set("n", "100");
		conf.set("p", "1000");
		@SuppressWarnings("deprecation")
			Job job = new Job(conf, "MatrixMultiply");
		job.setJarByClass(Workload_MatrixMultiply.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static class Map
		extends Mapper<ObjectId, BSONObject, Text, Text> {
			@Override
				public void map(ObjectId key, BSONObject value, Context context)
				throws IOException, InterruptedException {
					Configuration conf = context.getConfiguration();
					int m = Integer.parseInt(conf.get("m"));
					int p = Integer.parseInt(conf.get("p"));

					String c = String.valueOf(value.get("c"));
					int i = Integer.parseInt(String.valueOf(value.get("i")));
					int j = Integer.parseInt(String.valueOf(value.get("j")));
					int v = Integer.parseInt(String.valueOf(value.get("v")));

					// (M, i, j, Mij);
					Text outputKey = new Text();
					Text outputValue = new Text();

					// handle with if-else statement (efficient?)
					if (c.equals("M")) {
						for (int k = 0; k < p; k++) {
							outputKey.set(i + "," + k);
							// outputKey.set(i,k);
							outputValue.set(c + "," + j
									+ "," + v);
							// outputValue.set(M,j,Mij);
							context.write(outputKey, outputValue);
						}
					} else {
						// (N, j, k, Njk);
						for (int u = 0; u < m; u++) {
							outputKey.set(u + "," + j);
							outputValue.set("N," + i + ","
									+ v);
							context.write(outputKey, outputValue);
						}
					}
				}
		}

	public static class Reduce
		extends Reducer<Text, Text, Text, Text> {
			@Override
				public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					String[] value;
					//key=(i,k),
					//Values = [(M/N,j,V/W),..]
					HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
					HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
					for (Text val : values) {
						value = val.toString().split(",");
						if (value[0].equals("M")) {
							hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
						} else {
							hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
						}
					}
					int n = Integer.parseInt(context.getConfiguration().get("n"));
					float result = 0.0f;
					float m_ij;
					float n_jk;
					for (int j = 0; j < n; j++) {
						m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
						n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
						result += m_ij * n_jk;
					}
					if (result != 0.0f) {
						context.write(null,
								new Text(key.toString() + "," + Float.toString(result)));
					}
				}
		}

}
