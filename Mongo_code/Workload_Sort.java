import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class Sort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
        MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

        Job job = Job.getInstance(conf, "SortByValue");

        job.setNumReduceTasks(1);

        job.setJarByClass(Sort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<ObjectId, BSONObject, Text, Text> {

        Text id = new Text();
        private Text frequency = new Text();

        public void map(ObjectId key, BSONObject value, Context context) throws IOException, InterruptedException {
            String w = key.toString();
            String freq = String.valueOf(value.get("value"));

            id.set(w);
            frequency.set(freq);

            context.write(frequency, id);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for( Text value : values) {
              context.write(value, key);
            }
        }
    }
}
