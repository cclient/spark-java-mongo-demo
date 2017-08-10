package job.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import scala.Tuple2;

import java.util.Iterator;

public class JbyHadoop {
    public JbyHadoop() {
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("mongo spark").setMaster("local");
        final String mgohost = "172.32.255.27";

        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopconf = new Configuration();
        MongoConfig config = new MongoConfig(hadoopconf);
        config.setInputURI("mongodb://" + mgohost + ":27017/lewifi.auditOrigData");
        config.setQuery("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}");
        config.setOutputURI("mongodb://" + mgohost + ":27017/lewifi.monthtopurlwithcombine_spark_java");
        config.setOutputKey(Text.class);
        config.setOutputValue(BSONWritable.class);
        config.setInputFormat(MongoInputFormat.class);
        config.setOutputFormat(MongoOutputFormat.class);
        JavaPairRDD<Object, BSONObject> rdd = sc.newAPIHadoopRDD(hadoopconf, MongoInputFormat.class, Object.class, BSONObject.class);
        JavaPairRDD<Object, BSONObject> resultRDD = rdd
                .mapToPair(
                        (PairFunction<Tuple2<Object, BSONObject>, String, String>) e -> {
//	        	 System.out.print(e.toString());
                            String apmac = (String) e._2().get("apMac");
                            System.out.print(apmac);
                            String clientmac = (String) e._2().get("clientMac");
                            return new Tuple2<>(apmac, clientmac);
                        })
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<String, Iterable<String>>, Object, BSONObject>) t -> {
                    BSONObject bson = new BasicDBObject();
                    BSONObject subbson = new BasicDBObject();
                    Iterator<String> iter = t._2.iterator();
                    while (iter.hasNext()) {
                        subbson.put(iter.next(), "");
                    }
                    bson.put("apMac", t._1);
                    bson.put("clientMac", subbson);
                    return new Tuple2<>(t._1, bson);
                });
        //第一个参数 没用实际用途，但必须为可用路径
        resultRDD.saveAsNewAPIHadoopFile("hdfs:///sparktestdata", Object.class, Object.class, MongoOutputFormat.class, hadoopconf);
    }
}
