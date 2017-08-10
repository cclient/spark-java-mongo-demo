package job.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;
import org.bson.Document;
import scala.Tuple2;

import java.util.Iterator;

import static java.util.Collections.singletonList;


public class JbySpark {
    public JbySpark() {
    }

    public static void main(String[] args) {

        final String mgohost = "172.32.255.27";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://" + mgohost + ":27017/lewifi.auditOrigData")
                .config("spark.mongodb.output.uri", "mongodb://" + mgohost + ":27017/lewifi.monthtopurlwithcombine_spark_java")
                .config("mongo.input.query", "{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}")));
        System.out.println(aggregatedRdd.count());

        JavaRDD<Document> resultRDD = aggregatedRdd
                .mapToPair((PairFunction<Document, String, String>) document -> {
                    System.out.print(document.toString());
                    String apmac = document.getString("apMac");
                    System.out.print(apmac);
                    String clientmac = document.getString("clientMac");
                    return new Tuple2<>(apmac, clientmac);
                })
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<String, Iterable<String>>, String, BSONObject>) stringIterableTuple2 -> {
                    BSONObject bson = new BasicDBObject();
                    BSONObject subbson = new BasicDBObject();
                    Iterator<String> iter = stringIterableTuple2._2.iterator();
                    while (iter.hasNext()) {
                        subbson.put(iter.next(), "");
                    }
                    bson.put("apMac", stringIterableTuple2._1);
                    bson.put("clientMac", subbson);
                    return new Tuple2<>(stringIterableTuple2._1, bson);
                })
                .map((Function<Tuple2<String, BSONObject>, Document>) tuple2 -> {
                    Document td=new Document();
                    td.append(tuple2._1(), tuple2._2());
                    return td;
                });
        System.out.println(aggregatedRdd.first().toJson());
        MongoSpark.save(resultRDD);
    }
}
