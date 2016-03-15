package cn.com.gf.bdp


object Utils extends Logging {
  def insertSql(tbName: String, data: Map[String, String]): String = {
    val keys = data.keys.mkString("(", ",", ")")
    val values = data.values.map(x=> s"'$x'").mkString("(", ",", ")")
    val sql = s"INSERT INTO $tbName $keys VALUES $values"
    sql
  }

  def updateSql(
      tbName: String,
      data: Map[String, String],
      condition: Map[String, String]): String = {
    if(condition == null || condition.isEmpty) {
      throw new Exception("condition is empty")
    }
    val updateDataStr = data.map {
      case (key, value) =>
        s"$key = '$value'"
    }.mkString(",")

    val whereStr = condition.map {
      case (key, value) =>
        s"$key = '$value'"
    }.mkString(" and ")

    s"UPDATE $tbName SET $updateDataStr WHERE $whereStr"
  }

  def deleteSql(tbName: String, data:Map[String, String]): String = {
    if(data == null || data.isEmpty) {
      throw new Exception("Condition is empty")
    }
    val whereStr = data.map {
      case (key, value) =>
        s"$key = '$value'"
    }.mkString(" and ")

    s"DELETE FROM $tbName WHERE $whereStr"
  }

}
