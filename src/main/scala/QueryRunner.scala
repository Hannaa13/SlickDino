import slick.dbio.{DBIOAction, NoStream}
import slick.jdbc.JdbcProfile
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class QueryRunner(database: JdbcProfile#Backend#DatabaseDef) {

  def run[R](a: DBIOAction[R, NoStream, Nothing]) = {
    println(Await.result(database.run(a), Duration.Inf))
  }
}


