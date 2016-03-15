package cn.com.gf.bdp


import java.sql.{DriverManager, Connection}

import scalikejdbc._

object SetupJdbc extends Logging {
  def apply(driver: String, host: String, user: String, password: String): Unit = {
    logInfo(s"the driver is $driver, $host, $user, $password")
    Class.forName(driver)
    // DriverManager.registerDriver(Class.forName(driver).newInstance().asInstanceOf[Driver])
    ConnectionPool.singleton(host, user, password)
  }

  def getConnetion(driver: String, host: String, user: String, password: String): Connection = {
    Class.forName(driver)
    DriverManager.getConnection(host, user, password)
  }
}
