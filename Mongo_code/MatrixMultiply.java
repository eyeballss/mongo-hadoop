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

import com.mongodb.hadoop.splitter.*;

// mongo-java for aggregate
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import java.util.Arrays;
import java.util.List;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObject;
import org.json.JSONArray;
import org.json.JSONObject;


public class MatrixMultiply {
	public static void main(String[] args) throws Exception {

		/* make collection by query */
		MongoClient mongoClient = new MongoClient("mongo-router", 27017);
		MongoDatabase database = mongoClient.getDatabase("raw"); // db name
		MongoCollection<Document> coll = database.getCollection("small"); // collection name

		Block<Document> printBlock = new Block<Document>() {
			@Override
			public void apply(final Document document) {
				//System.out.println(document.toJson());
			}
		};

		// query
		List<? extends Bson> pipeline = Arrays.asList(
				new Document()
				.append("$group", new Document()
					.append("_id", "$j")
					.append("i", new Document()
						.append("$push", new Document()
							.append("$cond", Arrays.asList(new Document()
									.append("$and", Arrays.asList(new Document()
											.append("$not", Arrays.asList(
													"$k"
													)
												),
											new Document()
											.append("$gt", Arrays.asList(
													"$i", new BsonNull()
													)
												)
											)
										),
									new Document()
									.append("i", "$i")
									.append("v", "$v"),
									"$$REMOVE"
									)
								)
							)
							)
							.append("k", new Document()
									.append("$push", new Document()
										.append("$cond", Arrays.asList(new Document()
												.append("$and", Arrays.asList(new Document()
									.append("$not", Arrays.asList(
										"$i"
									)
								),
								new Document()
									.append("$gt", Arrays.asList(
										"$k", new BsonNull()
									)
								)
							)
						),
						new Document()
							.append("k", "$k")
							.append("v", "$v"),
							"$$REMOVE"
						)
					)
				)
			)
		),
		new Document()
			.append("$out", "tmp")
		);

		// execute
		coll.aggregate(pipeline)
			.allowDiskUse(true)
			.forEach(printBlock);

		System.out.println("temp collection made");


		Configuration conf = new Configuration();

		MongoConfigUtil.setInputURI(conf, "mongodb://" + args[0]);
		MongoConfigUtil.setOutputURI(conf, "mongodb://" + args[1]);

		//MongoConfigUtil.setSplitterClass(conf, ShardMongoSplitter.class);
		//MongoConfigUtil.setReadSplitsFromShards(conf, true);

		//MongoConfigUtil.setSplitterClass(conf, ShardChunkMongoSplitter.class);
		MongoConfigUtil.setSplitterClass(conf, ShardMongoSplitter.class);

		// query example
		//MongoConfigUtil.setQuery(conf, "{\"m\": \"M\"}");
		//MongoConfigUtil.setQuery(conf, "{\"j\": {\"$lt\": 1000 } }");


		@SuppressWarnings("deprecation")
			Job job = new Job(conf, "MatrixMultiply");

		job.setJarByClass(MatrixMultiply.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		job.waitForCompletion(true);
	}

	public static class Map
		extends Mapper<Integer, BasicDBObject, Text, IntWritable> {
			final static IntWritable one = new IntWritable(1);

			Text outputKey = new Text();
			IntWritable outputValue = new IntWritable();

			public void map(Integer key, BasicDBObject value, Context context)
				throws IOException, InterruptedException {

					JSONArray i_json = new JSONArray(value.getString("i"));
					JSONArray k_json = new JSONArray(value.getString("k"));

					JSONObject i_tmp = new JSONObject();
					JSONObject k_tmp = new JSONObject();
					String outKey;
					int outVal;
					for(int i=0; i<i_json.length(); i++) {
						i_tmp = i_json.getJSONObject(i);

						for(int j=0; j<k_json.length(); j++) {
							k_tmp = k_json.getJSONObject(j);

							outKey=String.valueOf(i_tmp.getInt("i"))+","+String.valueOf(k_tmp.getInt("k"));
							outVal=i_tmp.getInt("v")*k_tmp.getInt("v");

							outputKey.set(outKey);
							outputValue.set(outVal);

							context.write(outputKey, outputValue);

						}
					}
				}
		}

	public static class Reduce
		extends Reducer<Text, IntWritable, Text, IntWritable> {
			@Override
				public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
					int sum=0;

					for (IntWritable val : values) {
						sum += val.get();
					}
					context.write(key, new IntWritable(sum));

				}
		}
}
