package cn.com.gf.bdp

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite


class readConfSuite extends FunSuite {
  test("read conf") {
    val conf = ConfigFactory.load("application.conf")
    assert(conf.getString("kafka.brokers") == "localhost:9092,localhost:9093,localhost:9094")
    assert(conf.getLong("streaming.batch") == 2L)
  }
}
