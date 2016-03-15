package cn.com.gf.bdp

import org.joda.time.DateTime
import org.scalatest.FunSuite
import scalikejdbc._
case class Member(id: Long, name: Option[String], createdAt: DateTime)

object Member extends SQLSyntaxSupport[Member] {
  override val tableName = "members"
  def apply(rs: WrappedResultSet) = new Member(
    rs.long("id"), rs.stringOpt("name"), rs.jodaDateTime("created_at"))
}
class ScalikeJDBCSuite extends FunSuite {


  def insertsql(name: String): String = {
    s"insert into members (name, created_at) values ('${name}', current_timestamp)"
    //s"insert into members (name) values ('$name')"
  }

  ignore("insert to h2") {

    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:mem:hello", "user", "pass")
    implicit val session = AutoSession
    sql"""
       create table members (
         id serial not null primary key,
         name varchar(64),
         created_at timestamp
       )
      """.execute().apply()
    // insert initial data
    Seq("Alice", "Bob", "Chris") foreach { name =>
      val tablename = SQLSyntax.createUnsafely("members")
      // sql"${runsql}"
      val key = SQLSyntax.createUnsafely("name, created_at")
      val value = SQLSyntax.createUnsafely(s"'${name}', current_timestamp")

      val runsql = SQLSyntax.createUnsafely(insertsql(name))
      // sql"insert into ${tablename} ($key) values ($value)".update.apply()
      sql"$runsql".update.apply()
    }

    // for now, retrieves all data as Map value
    val entities: List[Map[String, Any]] = sql"select * from members".map(_.toMap).list.apply()

    // defines entity object and extractor
    import org.joda.time._


    // find all members
    val members = sql"select * from members".map(rs => Member(rs))
      .list
      .apply()
    println(members)

//    // use paste mode (:paste) on the Scala REPL
//    val m = Member.syntax("m")
//    val name = "Alice"
//    val alice: Option[Member] = withSQL {
//      select.from(Member as m).where.eq(m.name, name)
//    }.map(rs => Member(rs)).single.apply()
//    println(alice)

  }

}
