package job.mongo

import org.apache.spark.SparkContext
import com.mongodb.BasicDBObject
import com.mongodb.hadoop.MongoConfig
import com.mongodb.hadoop.MongoInputFormat
import com.mongodb.hadoop.MongoOutputFormat
import com.mongodb.hadoop.io.BSONWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.bson.BSONObject

object SbyHadoop {
  def main(args: Array[String]): Unit = {
    var conf =new SparkConf().setAppName("mongo spark").setMaster("local");
    val sc= SparkContext.getOrCreate(conf)
    val mgohost="172.32.255.27";
    val hadoopconf = new Configuration();
    val config = new MongoConfig(hadoopconf);
    config.setInputURI("mongodb://"+mgohost+":27017/lewifi.auditOrigData");
    config.setQuery("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}");
    config.setOutputURI("mongodb://"+mgohost+":27017/lewifi.monthtopurlwithcombine_spark_scala");

    config.setOutputKey(classOf[Text] )

    config.setOutputValue(classOf[BSONWritable]);
    config.setInputFormat(classOf[MongoInputFormat]);
    config.setOutputFormat(classOf[MongoOutputFormat[String,BSONObject]]);
    val rdd=sc.newAPIHadoopRDD(hadoopconf, classOf[MongoInputFormat],classOf[Object],classOf[BSONObject] )
    rdd
      .map(x=> (x._2.asInstanceOf[BSONObject].get("apMac").asInstanceOf[String],x._2.asInstanceOf[BSONObject].get("clientMac").asInstanceOf[String]))
      .groupByKey()
      .map(t =>{
        var bson=new BasicDBObject();
        var subbson=new BasicDBObject();
        var iter=t._2.iterator;
        while(iter.hasNext){
          subbson.put(iter.next(), "");
        }
        bson.put("apMac", t._1);
        bson.put("clientMac", subbson);
        (t._1,bson)
      })
      .saveAsNewAPIHadoopFile("hdfs:///sparktestdata", classOf[Object],classOf[Object],classOf[MongoOutputFormat[String,BSONObject]], hadoopconf)
  }
}

