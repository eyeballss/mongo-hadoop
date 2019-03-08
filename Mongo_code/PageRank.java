import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.splitter.ShardMongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class PageRank {

	private static final double DAMPING_FACTOR = 0.85;
	private static final String SEPARATOR = "\t";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		MongoConfigUtil.setInputURI(conf, "mongodb://mongo-router/dataset.result");
		MongoConfigUtil.setOutputURI(conf, "mongodb://mongo-router/dataset.data.out");
		MongoConfigUtil.setSplitterClass(conf, ShardMongoSplitter.class);

		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://mongo-router:27017"));

		long start = System.currentTimeMillis();

		if (true) {
			try {

				MongoDatabase database = mongoClient.getDatabase("dataset");
				MongoCollection<Document> collection = database.getCollection("data");

				Block<Document> processBlock = new Block<Document>() {
					@Override
					public void apply(final Document document) {
					}
				};
				List<? extends Bson> pipeline = Arrays.asList(
						new Document().append("$group",
								new Document().append("_id", "$from").append("init", new Document().append("$min", "1"))
										.append("to", new Document().append("$push", "$to"))),
						new Document().append("$out", "result"));
				collection.aggregate(pipeline).allowDiskUse(true).forEach(processBlock);
			} catch (MongoException e) {
				// handle MongoDB exception
e.printStackTrace();
return;
			}
		} // if

		long end = System.currentTimeMillis(); 
		System.out.println( "query time : " + ( end - start )/1000.0 +" sec");

		Job job2 = job2Setting(conf, args, "", ".0");
		job2.waitForCompletion(true);

		int i = 0;
		Job job3;
		for (i = 1; i <= 1; i++) {
			job3 = jobRankSetting(conf, args, "." + (i - 1), "." + i);
			job3.waitForCompletion(true);
		}

		Job job4 = jobCleanUpSetting(conf, args, "." + (i - 1), ".out");
		job4.waitForCompletion(true);

	}

	public static class PretreatmentMap extends Mapper<Integer, BasicDBObject, Text, Text> {
		public void map(Integer key, BasicDBObject value, Context context) throws IOException, InterruptedException {

			Text keyText = new Text(String.valueOf(key));

			String to = value.getString("to");
			String tos[] = to.subSequence(1, to.length() - 1).toString().split(",");

			StringBuilder sb = new StringBuilder();
			sb.append(value.getString("init"));
			for (String t : tos) {
				sb.append(SEPARATOR);
				sb.append(t);
			}

			Text valueText = new Text(sb.toString());

			context.write(keyText, valueText);
		}
	}

	public static class PageRankMap extends Mapper<Text, Text, Text, Text> {

		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value); // original input data

			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), SEPARATOR);
			if (parts.length >= 2) {
				int qnsah = parts.length - 1;
				if (qnsah == 0)
					return;

				double outboundPageRank = Double.parseDouble(parts[0]) / (double) qnsah;
				for (int i = 1; i <= qnsah; i++) {
					String outbound = parts[i];
					Text outKey = new Text(outbound);
					Text outValue = new Text(String.valueOf(outboundPageRank).trim());
					context.write(outKey, outValue);
				}
			}
		}
	}

	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double totalPageRank = 0;
			String outbounds = null;

			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString().trim(), SEPARATOR);

				if (parts.length < 2) { // rank value
					totalPageRank += Double.parseDouble(parts[0]);
				} else { // original input data
					outbounds = value.toString();
				}
			}

			// this node doesn't have any inbound links.
			if (outbounds == null) {
				return;
			}

			double dampingFactor = (1d - DAMPING_FACTOR);
			double newPageRank = dampingFactor + (DAMPING_FACTOR) * (totalPageRank);

			String[] result = outbounds.split(SEPARATOR);

			StringBuilder sb = new StringBuilder();
			sb.append(newPageRank);
			for (int i = 1; i < result.length; i++) {
				sb.append(SEPARATOR + result[i]);
			}

			context.write(key, new Text(sb.toString()));
			totalPageRank = 0;
		}
	}

	private static class CleanUpMap extends Mapper<Text, Text, Text, BSONWritable> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String values[] = line.split(SEPARATOR);

			BasicBSONObject output = new BasicBSONObject();
			BSONWritable result = new BSONWritable();
			output.put("rank", values[0]);
			result.setDoc(output);

			context.write(key, result);
		}

	}

	private static Job job2Setting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "pretreatment");
		job.setJarByClass(PageRank.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		job.setMapperClass(PretreatmentMap.class);
		job.setInputFormatClass(MongoInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0] + to));
		return job;
	}

	private static Job jobRankSetting(Configuration conf, String[] args, String from, String to) throws IOException {
		Job job = Job.getInstance(conf, "page rank");
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

	private static Job jobCleanUpSetting(Configuration conf, String[] args, String from, String to) throws IOException {

		Job job = Job.getInstance(conf, "PageRankFinishing");
		job.setJarByClass(PageRank.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(CleanUpMap.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0] + from));

		return job;
	}
}

