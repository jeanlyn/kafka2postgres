package cn.com.gf.bdp

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, GivenWhenThen, FunSuite}
import org.scalatest.concurrent.Eventually
import scalikejdbc._
import scalikejdbc.AutoSession

import scala.collection.mutable.ListBuffer
import scala.collection.mutable


class kafka2postgresSuite extends FunSuite
    with streamingWithH2DB with GivenWhenThen with Matchers with Eventually {

  val appconf = ConfigFactory.load("application.conf")
  val jdbcDriver = appconf.getString("jdbc.driver")
  val jdbcUrl = appconf.getString("jdbc.url")
  val jdbcUser = appconf.getString("jdbc.user")
  val jdbcPassword = appconf.getString("jdbc.password")



  test("parseAndGetSql") {
    var message=
      """
        |{"sequence":123,"type":"insert","table":"ENTRUST_TEST","data":{"INIT_DATE":"20160218","ENTRUST_NO":"16583"}}
      """.stripMargin
    val result = kafka2postgres.parseAndGetSql(message)
    val except = "INSERT INTO ENTRUST_TEST (INIT_DATE,ENTRUST_NO) VALUES ('20160218','16583')"
    assert(result == except)

    message =
      """{"sequence":"15365","type":"delete","table":"TEST1","data":{"EMPNO":"7369","ENAME":"jiuqiao","JOB":"CLERK","MGR":"7902","HIREDATE":"19801217","SAL":"800","COMM":"","DEPTNO":"20"}} """
    println(kafka2postgres.parseAndGetSql(message))

  }


  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(100, Millis)))

  test("Sample set should be counted") {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    val stream = ssc.queueStream(lines)

    kafka2postgres.saveMessageToDB(ssc, stream, jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)

    ssc.start()


    When("first set of words queued to insert")
    lines += sc.makeRDD(
      Seq(
        """{"sequence":"1","type":"insert","table":"TEST1","data":{"EMPNO":"7369","ENAME":"SMITH","JOB":"CLERK","MGR":"7902","HIREDATE":"19801217","SAL":"800","COMM":"","DEPTNO":"20"}} """,
        """{"sequence":"2","type":"insert","table":"TEST1","data":{"EMPNO":"7499","ENAME":"ALLEN","JOB":"SALESMAN","MGR":"7698","HIREDATE":"19810220","SAL":"1600","COMM":"300","DEPTNO":"30"}} """))

    Then("insert data first slide")
    clock.advance(batchDuration.milliseconds)
    eventually {
      getDbCount should equal(2)
    }

    When("second set of words queued to insert")
    lines += sc.makeRDD(
      Seq(
        """{"sequence":"3","type":"insert","table":"TEST1","data":{"EMPNO":"7521","ENAME":"WARD","JOB":"SALESMAN","MGR":"7698","HIREDATE":"19810222","SAL":"1250","COMM":"500","DEPTNO":"30"}} """,
        """{"sequence":"4","type":"insert","table":"TEST1","data":{"EMPNO":"7566","ENAME":"JONES","JOB":"MANAGER","MGR":"7839","HIREDATE":"19810402","SAL":"2975","COMM":"","DEPTNO":"20"}} """))

    Then("insert data after second slide, the count should be 4")
    clock.advance(batchDuration.milliseconds)
    eventually {
      getDbCount should equal(4)
    }

    When("update the data comming")
    lines += sc.makeRDD(
      Seq(
      """{"sequence":"3589","type":"update","table":"TEST1","data":{"EMPNO":"7369","ENAME":"jiuqiao","JOB":"CLERK","MGR":"7902","HIREDATE":"19801217","SAL":"800","COMM":"","DEPTNO":"20"},"where":{"EMPNO":"7369","ENAME":"SMITH","JOB":"CLERK","MGR":"7902","HIREDATE":"19801217","SAL":"800","COMM":"","DEPTNO":"20"}}""")
    )
    Then("update data after one batch")
    clock.advance(batchDuration.milliseconds)
    eventually {
      getENAME("7369") should equal("jiuqiao")
    }

    When("delete data comming")
    lines += sc.makeRDD(
      Seq(
      """{"sequence":"15365","type":"delete","table":"TEST1","data":{"EMPNO":"7369","ENAME":"jiuqiao","JOB":"CLERK","MGR":"7902","HIREDATE":"19801217","SAL":"800","COMM":"","DEPTNO":"20"}}""")
    )
    Then("table count should be 3")
    clock.advance(batchDuration.milliseconds)
    eventually {
      getDbCount should equal(3)
    }

    When("given the wrong message")
    lines += sc.makeRDD(
      Seq(
      """{"sequence":"5","type":"insert","table":"TEST1","data":{"EMPNO":"7654","ENAME":"MARTIN","JOB":"SALESMAN","MGR":"7698","HIREDATE":"19810928","SAL":"1250","COMM":"1400","DEPTNO":"30"}} """,
      """{"sequence":"5","type":"wrongOperator","table":"TEST1","data":{"EMPNO":"7654","ENAME":"MARTIN","JOB":"SALESMAN","MGR":"7698","HIREDATE":"19810928","SAL":"1250","COMM":"1400","DEPTNO":"30"}} """,
      """{"sequence":"6","type":"insert","table":"TEST1","data":{"EMPNO":"7698","ENAME":"BLAKE","JOB":"MANAGER","MGR":"7839","HIREDATE":"19810501","SAL":"2850","COMM":"","DEPTNO":"30"}} """
      )
    )
    Then("It should equal 5")
    clock.advance(batchDuration.milliseconds)
    eventually {
      getDbCount should equal(5)
    }


    def getDbCount: Int = {
      val re = DB.autoCommit { implicit session =>
        sql"SELECT count(1) as c FROM TEST1".map(rs => rs.int("c")).first().apply()
      }

      re.get
    }

    def getENAME(EMPNO: String): String = {
      val re = DB.autoCommit { implicit  session =>
        sql"SELECT * FROM TEST1 WHERE EMPNO=$EMPNO".map(rs => rs.string("ENAME")).first().apply()
      }
      re.get
    }

  }
}
