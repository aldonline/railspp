package util

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import net.sourceforge.schemaspy._
import model.Database

/**
 * Postgres DB wrapper with metadata reflection capabilities
 * via spyDB
 */
class PGDB ( host:String, dbname:String, user:String ) {

  private def jarForClass(c:String) = Class.forName(c).getProtectionDomain.getCodeSource.getLocation
  private lazy val driverPath = jarForClass("org.postgresql.Driver")

  private val conn_str = s"jdbc:postgresql://$host/$dbname?user=$user"
  private val x = classOf[org.postgresql.Driver]

  implicit class FFX[X]( x:X ){ def ->>(f: X => Unit) = { f(x); x } }
  
  lazy val spyDB = {
    val args = s"-t pgsql -host $host -u $user -s public -db $dbname -o /tmp/schemaspy-output -nohtml".split(" ")
    new SchemaAnalyzer() analyze new Config(args) ->> { _ setDriverPath driverPath.toString }
  }

  def sql( str:String ):Stream[ResultSet] = withStatement { s =>
    val rs = s executeQuery str
    new Iterator[ResultSet] { def hasNext = rs.next ;  def next = rs }.toStream
  }
  
  private def withStatement[R]( f:(Statement) => R ): R =
    withConn { c => f( c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) ) }
  
  private def withConn[R]( f:(java.sql.Connection) => R ):R = {
    val conn = DriverManager getConnection conn_str
    try f(conn) finally conn.close()
  }
  
}