# spark-java-mongo-demo
spark
通过 mongo-hadoop
分析 mongodb数据
java写成


执行方式
root@debian01:/usr/local/spark-1.3.1-bin-hadoop2.6# bin/spark-submit --class "sparkfisrttest.cdpspark.App" --packages org.mongodb.mongo-hadoop:mongo-hadoop-core:1.3.1,org.mongodb:mongodb-driver:3.0.1,org.mongodb:mongo-java-driver:3.0.1 ~/hadoop-spark-mongo-examples.jar 

个人测试通过

