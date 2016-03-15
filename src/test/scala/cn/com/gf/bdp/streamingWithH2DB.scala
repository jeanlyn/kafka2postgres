package cn.com.gf.bdp

import com.typesafe.config.ConfigFactory
import org.h2.tools.Server
import org.scalatest.Suite
import scalikejdbc._


trait streamingWithH2DB extends StreamingSpec {
  this: Suite =>


  override def beforeAll(): Unit = {
    super.beforeAll()
    val appconf = ConfigFactory.load("application.conf")
    val jdbcDriver = appconf.getString("jdbc.driver")
    val jdbcUrl = appconf.getString("jdbc.url")
    val jdbcUser = appconf.getString("jdbc.user")
    val jdbcPassword = appconf.getString("jdbc.password")
    SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)
    DB.autoCommit { implicit session =>
      sql"""
       create table IF NOT EXISTS TEST1 (
          ENAME varchar(20),
          EMPNO varchar(20),
          SAL varchar(20),
          MGR varchar(20),
          DEPTNO varchar(20),
          JOB varchar(20),
          COMM varchar(20),
          DNAME varchar(20),
          HIREDATE varchar(20),
          LOC varchar(20))
      """.execute().apply()
    }

  }

  def createtable(): Unit = {


  }

  override def  afterAll(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""
       DROP TABLE TEST1
      """.execute().apply()
    }
    super.afterAll()
  }


}
