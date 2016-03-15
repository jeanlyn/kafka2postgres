# kafka2postgres
Read data from kafka and sink data to postgres. It has already include unit test. You can read `kafka2postgresSuite.scala` for more detail.

## Run unit test
mvn clean test

## Package
mvn clean package

## Configuration
File: conf/spark.conf
This is use for config the informations includes:
1. topic of kafka
2. brokers address of kafka
3. Spark streaming checkpoint directory 
4. batch time of spark streaming
5. jdbc of postgres
