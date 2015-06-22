
import java.util.Date

import com.datastax.driver.core._
import org.cassandraunit.utils.EmbeddedCassandraServerHelper


object CassandraAlterTable extends App with CassandraMystery{

  createTable()
  addRow()
  val session: Session = createSession()
  val statement: PreparedStatement = session.prepare("select * from mystery")

  require(session.execute(statement.bind()).all().get(0).getDate("d") == new Date(1433116800000L))

  session.execute("alter table mystery add a VARCHAR")

  require(session.execute(statement.bind()).all().get(0).getDate("d") == new Date(1433116800000L))

}

trait CassandraMystery {

  def createTable() = {
    executeCqlScript( """
      CREATE TABLE mystery (
      key VARCHAR,
      b VARCHAR,
      d TIMESTAMP,
      PRIMARY KEY ((key)))"""
    )
  }

  def addRow() = {
    executeCqlScript("insert into mystery(key, b, d) values ('key', 'a', '2015-06-01 00:00:00+0000')")
  }

  val cluster = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
    val cluster = new Cluster.Builder()
      .addContactPoints("127.0.0.1")
      .withPort(9142)
      .withSocketOptions(new SocketOptions().setKeepAlive(true)).build()

    cluster.connect().execute("CREATE KEYSPACE tst WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3};")

    cluster
  }

  def createSession(): Session = {
    cluster.connect("tst")
  }

  def executeCqlScript(cqlScript: String): Unit = {
    createSession().execute(cqlScript)
  }

}
