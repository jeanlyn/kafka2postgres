package cn.com.gf.bdp

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import com.typesafe.config.{Config, ConfigFactory}
import net.liftweb.json._

import scala.util.control.NonFatal


object kafka2postgres extends Logging {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load("spark.conf")
    val topic =conf.getString("kafka.topics").split(",").toSet
    val kafkaConf = Map(
      "metadata.broker.list" -> conf.getString("kafka.brokers"),
      // start from the smallest available offset, ie the beginning of the kafka log
      "auto.offset.reset" -> conf.getString("kafka.offset")
    )
    val jdbcDriver = conf.getString("jdbc.driver")
    val jdbcUrl = conf.getString("jdbc.url")
    val jdbcUser = conf.getString("jdbc.user")
    val jdbcPassword = conf.getString("jdbc.password")
    val checkpointDir = conf.getString("checkpointDir")
    val batchDuration = conf.getLong("streaming.batch")

    val ssc = StreamingContext.getOrCreate(
      checkpointDir,
      setUpSsc(topic, kafkaConf, jdbcDriver,jdbcUrl,
        jdbcUser, jdbcPassword, checkpointDir, batchDuration) _)

    ssc.start()
    ssc.awaitTermination()

  }

  def setUpSsc(
    topic: Set[String],
    kafkaConf: Map[String, String],
    jdbcDriver: String,
    jdbcUrl: String,
    jdbcUser: String,
    jdbcPassWord: String,
    checkPointDir: String,
    batchDuration: Long
    )(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf(), Seconds(batchDuration))

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, topic).map(_._2)

    saveMessageToDB(ssc, stream, jdbcDriver, jdbcUrl, jdbcUser, jdbcPassWord)

    ssc.checkpoint(checkPointDir)
    ssc
  }

  // for test
  def saveMessageToDB(
      ssc: StreamingContext,
      stream: DStream[String],
      jdbcDriver: String,
      jdbcUrl: String,
      jdbcUser: String,
      jdbcPassWord: String): Unit = {
    stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        val con = SetupJdbc.getConnetion(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassWord)

        //DB.autoCommit { implicit session =>
          iter.foreach { message =>
            try {
              //val sqlStatement = SQLSyntax.createUnsafely(parseAndGetSql(message))
              //sql"$sqlStatement".update.apply
              val stmt = con.createStatement()
              stmt.execute(parseAndGetSql(message))
            } catch {
              case sqle: SQLOperateException =>
                logError(sqle.getMessage)
              case NonFatal(e) =>
                logError(s"Message $message unhandle. Because" + e.getMessage)
            }
          }
        con.close()
        //}
      }
    }

  }


  def parseAndGetSql(message: String): String = {
    // parse json
    val json = parse(message).values.asInstanceOf[Map[String, _]]
    var sql =""
    val op = json("type").toString.toLowerCase()
    val data = json("data").asInstanceOf[Map[String, String]]
    val tablename = json("table").toString

    if(op == "insert") {
      sql = Utils.insertSql(tablename, data)
    } else if (op == "update") {
      val whereCondition = json("where").asInstanceOf[Map[String, String]]
      sql = Utils.updateSql(tablename, data, whereCondition)
    } else if (op == "delete") {
      sql = Utils.deleteSql(tablename, data)
    } else {
      throw new SQLOperateException("Unsupport operation:" + op + s",message: $message")
    }

    sql
  }

}
