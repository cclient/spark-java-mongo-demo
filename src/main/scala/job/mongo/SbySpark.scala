package job.mongo

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document


object SbySpark {
  def main(args: Array[String]): Unit = {

    val mgohost = "172.32.255.27"
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://" + mgohost + ":27017/lewifi.auditOrigData")
      .config("spark.mongodb.output.uri", "mongodb://" + mgohost + ":27017/lewifi.monthtopurlwithcombine_spark_java")
      .config("mongo.input.query", "{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}")
      .getOrCreate();

    val rdd=MongoSpark.load(spark.sparkContext);
    val aggregatedRdd=rdd.withPipeline(List(Document.parse("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}")))
    val resultRDD=aggregatedRdd
      .map[(String, String)](document=>Tuple2(document.getString("apMac"),document.getString("clientMac")))
      .groupByKey()
      .map(t=>{
        val bson= new Document();
        val subbson=new Document();
        val iter=t._2.iterator;
        while(iter.hasNext){
          subbson.put(iter.next(), "");
        }
        bson.put("apMac", t._1);
        bson.put("clientMac", subbson);
        (t._1,bson)
      })
      .map[Document](tuple2 => {
        val td=new Document();
        td.append(tuple2._1, tuple2._2);
        td
      })
//    System.out.println(resultRDD.first().toJson());
    MongoSpark.save(resultRDD);
  }
}

