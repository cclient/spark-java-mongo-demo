# spark-java-mongo-demo
spark
通过 mongo-hadoop
分析 mongodb数据
java写成

mongodb 原始数据
{
    "_id" : ObjectId("54d83f3548c9bc218e056ce6"),
    "apMac" : "aa:bb:cc:dd:ee:ff",
    "proto" : "http",
    "url" : "extshort.weixin.qq.com",
    "clientMac" : "ff:ee:dd:cc:bb:aa"
}

输出结果

mvn clean scala:compile compile package

执行方式
spark-submit --class "sparkfisrttest.cdpspark.App" --packages org.mongodb.mongo-hadoop:mongo-hadoop-core:1.3.1,org.mongodb:mongodb-driver:3.0.1,org.mongodb:mongo-java-driver:3.0.1 ~/hadoop-spark-mongo-examples.jar

demo早期基于hadoop 的 mongo driver

mongo-hadoop-core
 
近期发现有spark官方的connector

https://docs.mongodb.com/spark-connector/current/

便加了基于mongo-spark-connector的rdd示例(dataset和sql尚不熟悉)
