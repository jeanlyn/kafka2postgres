package cn.com.gf.bdp

import net.liftweb.json._
import org.scalatest.FunSuite


class UtilSuite extends FunSuite {

//  test("parse json from file") {
//    val result = collection.mutable.Map[String, collection.Set[String]]()
//    val key = scala.collection.mutable.Set[String]()
//    val data = sc.textFile("src/test/resources/data.txt")
//    data.map { x =>
//      val json = parse(x).values.asInstanceOf[Map[String, _]]
//      (json("table").toString, json("data").asInstanceOf[Map[String, String]].keySet)
//    }.collect.foreach { x =>
//      var keys = result.getOrElse(x._1, collection.mutable.Set[String]())
//      keys ++= x._2
//      result.put(x._1, keys)
//    }
//    println(result)
//    result.foreach { x =>
//      val s = x._2.map(x => s"$x varchar(128)").mkString(s"CREATE TABLE ${x._1} (\n  ",",\n  ",")")
//      println(s)
//    }

//  }

  test("parse json") {
    val str =
      """
        |{"sequence":123,"type":"insert","table":"ENTRUST","data":{"INIT_DATE":"20160218"}}
      """.stripMargin
    val json = parse(str)
    println(json)
    val b = json.values.asInstanceOf[Map[String, _]]
    assert(b("type") == "insert")
  }

  test("parse json2") {
    val str =
      """
        |{"sequence":"1","type":"insert","table":"TEST1","data":{"EMPNO":"7369","ENAME":"SMITH","JOB":"CLERK","MGR":"7902","HIREDATE":"19801217","SAL":"800","COMM":"","DEPTNO":"20"}}
      """.stripMargin
    val json = parse(str)
    println(json)
  }

  test("insertSql") {
    val map = Map("key1" -> "value1", "key2" -> "value2")
    val tablename = "tbname"
    val except = s"INSERT INTO $tablename (key1,key2) VALUES ('value1','value2')"
    println(except)
    assert(Utils.insertSql(tablename, map) == except)
  }

  test("updataSql") {
    val data = Map("key1" -> "value1", "key2" -> "value2")
    val where = Map("key3" -> "value3", "key4" -> "value4")
    val tablename = "tbname"
    val except =
      s"UPDATE $tablename SET key1 = 'value1',key2 = 'value2' WHERE key3 = 'value3' and key4 = 'value4'"
    println(Utils.updateSql(tablename, data, where))
    assert(Utils.updateSql(tablename, data, where) == except)
  }

  test("deleteSql") {
    val map = Map("key1" -> "value1", "key2" -> "value2")
    val tablename = "tbname"
    val except = s"DELETE FROM $tablename WHERE key1 = 'value1' and key2 = 'value2'"
    println(Utils.deleteSql(tablename, map))
    assert(Utils.deleteSql(tablename, map) == except)
  }


}
