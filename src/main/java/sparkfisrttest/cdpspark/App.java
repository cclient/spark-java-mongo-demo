package sparkfisrttest.cdpspark;



import java.util.Iterator;
import java.util.List;

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
//import com.mongodb.hadoop.util.MongoTool;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;








import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.BSONObject;



/**
 * Hello world!
 *
 */
public class App 
{
	public App(){
		
	}
    public static void main( String[] args )
    {
    	SparkConf conf = new SparkConf().setAppName("mongo apark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Configuration hadoopconf = new Configuration();
		MongoConfig config = new MongoConfig(hadoopconf);
		config.setInputURI("mongodb://172.16.231.1:27017/lewifi.auditOrigData");
		config.setOutputURI("mongodb://172.16.231.1:27017/lewifi.monthtopurlwithcombine");
		config.setInputFormat(MongoInputFormat.class);
		config.setOutputFormat(MongoOutputFormat.class);		
		JavaPairRDD  rdd=sc.newAPIHadoopRDD(hadoopconf, MongoInputFormat.class, Object.class, BSONObject.class);
//		rdd.map(new Function<Object, BSONObject>() {
//		      public BSONObject call(BSONObject integer) {
//		        double x = Math.random() * 2 - 1;
//		        double y = Math.random() * 2 - 1;
//		        return (x * x + y * y < 1) ? 1 : 0;
//		      }
//		    });
		JavaRDD  jdd=	rdd.values();
		List ls=	jdd.collect();
		Iterator i = ls.iterator(); 
		while(i.hasNext()){
			  com.mongodb.BasicDBObject obj=(BasicDBObject) i.next();
			  String str= obj.toString();
			System.out.println(obj.getClass());
			System.out.println(str);	
		}
    }
}
