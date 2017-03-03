package sparkfisrttest.cdpspark;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.SparkConf;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.bson.BSONObject;

import scala.Tuple2;
public class App {
	public App() {
	}
	public static void main( String[] args )
	{
		SparkConf conf = new SparkConf().setAppName("mongo apark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Configuration hadoopconf = new Configuration();
		MongoConfig config = new MongoConfig(hadoopconf);
		config.setInputURI("mongodb://172.16.231.1:27017/lewifi.auditOrigData");
		config.setQuery("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}");
		config.setOutputURI("mongodb://172.16.231.1:27017/lewifi.monthtopurlwithcombine2");
		config.setOutputKey(Text.class);
		config.setOutputValue(BSONWritable.class);
		config.setInputFormat(MongoInputFormat.class);
		config.setOutputFormat(MongoOutputFormat.class);
		JavaPairRDD<Object,BSONObject>  rdd=sc.newAPIHadoopRDD(hadoopconf, MongoInputFormat.class, Object.class, BSONObject.class);
		JavaPairRDD<Object, BSONObject> resultRDD = rdd.mapToPair(
				new PairFunction<Tuple2<Object, BSONObject>, String, String>() {
					public Tuple2<String, String> call(Tuple2<Object, BSONObject> e) {
//	        	 System.out.print(e.toString());
						String apmac=(String) e._2().get("apMac");
						System.out.print(apmac);
						String clientmac=(String) e._2().get("clientMac");
						return new Tuple2<String, String>(apmac,clientmac);
					}
				}).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<String>>,Object,BSONObject>(){
			public Tuple2<Object, BSONObject> call(
					Tuple2<String, Iterable<String>> t) throws Exception {
				BSONObject bson=new BasicDBObject();
				BSONObject subbson=new BasicDBObject();
				Iterator<String> iter=t._2.iterator();
				while(iter.hasNext()){
					subbson.put(iter.next(), "");
				}
				bson.put("apmac", t._1);
				bson.put("clientmac", subbson);
				return new Tuple2<Object, BSONObject>(t._1,bson);
			}

		});
		//第一个参数 没用实际用途，但必须为可用路径
		resultRDD.saveAsNewAPIHadoopFile("hdfs:///sparktestdata", Object.class,Object.class,MongoOutputFormat.class, hadoopconf);
	}
}
