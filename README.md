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

执行方式
root@debian01:/usr/local/spark-1.3.1-bin-hadoop2.6# bin/spark-submit --class "sparkfisrttest.cdpspark.App" --packages org.mongodb.mongo-hadoop:mongo-hadoop-core:1.3.1,org.mongodb:mongodb-driver:3.0.1,org.mongodb:mongo-java-driver:3.0.1 ~/hadoop-spark-mongo-examples.jar

