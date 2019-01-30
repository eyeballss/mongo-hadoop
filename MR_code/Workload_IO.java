import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.splitter.ShardMongoSplitter;
import com.mongodb.hadoop.splitter.ShardChunkMongoSplitter;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Workload_IO{
        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();

                //MongoConfigUtil.setInputURI(conf, "mongodb://"+args[0]+"?readPreference=nearest");
                //MongoConfigUtil.setOutputURI(conf, "mongodb://"+args[1]);
                //MongoConfigUtil.setSplitterClass(conf, ShardChunkMongoSplitter.class);
                //MongoConfigUtil.setSplitterClass(conf, ShardMongoSplitter.class);

                Job job = Job.getInstance(conf, "Worklaod_IO");

        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(Text.class);

        	job.setMapperClass(Map.class);
        	//job.setReducerClass(Reduce.class);

        	job.setInputFormatClass(TextInputFormat.class);
        	job.setOutputFormatClass(TextOutputFormat.class);

        	// Setting the input and output locations
        	FileInputFormat.addInputPath(job, new Path(args[0]));
        	FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setJarByClass(Workload_IO.class);

                //job.setMapOutputKeyClass(Text.class);
                //job.setMapOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

        }

        public static class Map extends Mapper<Object, Text, Text, Text> {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                }
        }

        public static class Reduce extends Reducer<Text, Text, Text, Text> {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                }
        }
}


