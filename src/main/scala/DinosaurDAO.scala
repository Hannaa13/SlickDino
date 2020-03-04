import java.time.Instant

import slick.dbio.{DBIO, Effect}
import slick.jdbc
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api._
import slick.sql.{FixedSqlAction, SqlAction}

import scala.concurrent.ExecutionContext

trait DinosaurDAO {

  def init: DBIO[Unit]

  protected def initIdGenerator: SqlAction[Int, NoStream, Effect]

  def selectALL: DBIO[Seq[Dinosaur]]

  def create(dinosaur: Dinosaur): DBIO[Dinosaur]

  def update(dinosaur: Dinosaur): DBIO[Dinosaur]

  def delete(dinosaur: Dinosaur): DBIO[Unit]

  def deleteAll: DBIO[Unit]

  def deleteAll(ids: Set[Int]): DBIO[Unit]

  def take(n: Int): DBIO[Seq[Dinosaur]]

  def mapDinosaurParam: DBIO[Seq[BodyParams]]

  def findAndUpgrade: Unit

  def leftJoinExample: DBIO[Seq[(Dinosaur, Option[EventLogs])]]

  def rightJoinExample: DBIO[Seq[(Option[Dinosaur], EventLogs)]]

  def fullJoinExample: DBIO[Seq[(Option[Dinosaur], Option[EventLogs])]]

  def innerJoinExample: DBIO[Seq[(Dinosaur, EventLogs)]]

  def crossJoinExample: DBIO[Seq[(Dinosaur, EventLogs)]]

  def leftJoinFilter: DBIO[Seq[(Dinosaur, EventLogs)]]

  def leftJoinTime: DBIO[Seq[TimeOfCreation]]

}

class DinosaurDAOImpl(dinosaurModel: DinosaurModel with EventLogsModel)
                     (implicit executionContext: ExecutionContext) extends DinosaurDAO {

  import dinosaurModel.{dinosaurs, dinosaursSeq, eventLogs}

  // Склеиваем все что нужно для создания, не отдельно же вызывать?)
  def init: DBIO[Unit] = {
    for {
      _ <- (dinosaurs.schema ++ eventLogs.schema).create
      _ <- initIdGenerator
    } yield {}
  }

  protected def initIdGenerator: SqlAction[Int, NoStream, Effect] = sqlu"""create sequence "dino_id_seq" start with 1 increment by 1;"""

  def selectALL: DBIO[Seq[Dinosaur]] = dinosaurs.result


  def create(dinosaur: Dinosaur): DBIO[Dinosaur] = {
    for {
      newId <- dinosaursSeq.next.result
      dino = dinosaur.copy(id = Some(newId))
      _ <- dinosaurs += dino
      _ <- eventLogs += EventLogs(None, newId, Some(Instant.now), None, None)
    } yield dino
  }

  def update(dinosaur: Dinosaur): DBIO[Dinosaur] = {
    for {
      _ <- dinosaurs
        .update(dinosaur)
      _ <- dinosaur.id.map(idReal => eventLogs += EventLogs(None, idReal, None, None, Some(Instant.now))).getOrElse(DBIO.successful(()))
    } yield dinosaur
  }


  def delete(dinosaur: Dinosaur): DBIO[Unit] = {
    for {
      _ <- eventLogs
        .filter(_.dinosaurId === dinosaur.id.bind).delete
      _ <- dinosaurs
        .filter(_.id === dinosaur.id.bind).delete
      // _ <- dinosaur.id.map(idDel => eventLogs += EventLogs(None, idDel, None, Some(Instant.now), None)).getOrElse(DBIO.successful(()))
    } yield ()
  }

  def deleteAll: DBIO[Unit] = {
    (for {
      _ <- DBIO.seq(eventLogs.delete)
      _ <- DBIO.seq(dinosaurs.delete)
    } yield ()).transactionally
  }

  def deleteAll(ids: Set[Int]): DBIO[Unit] = {
    (for {
      _ <- eventLogs.filter(_.dinosaurId inSet ids).delete
      _ <- dinosaurs.filter(_.id inSet ids).delete
    } yield ()).transactionally
  }


  //Берет первые n элементов. Удобная штука, когда надо обрабатывать что-то по очереди, например некие заявки
  def take(n: Int): DBIO[Seq[Dinosaur]] = {
    dinosaurs
      .take(n)
      .result
  }


  //Конвертируем результат в новый кейс класс. Фактически select лишь нескольких полей.
  // Полезная штука, когда у нас в таблице дофига полей, а значения нужны лишь из нескольких

  def mapDinosaurParam: DBIO[Seq[BodyParams]] = {
    dinosaurs
      .map(d => (d.weight, d.height))
      .result
      //.map(_.map(v => BodyParams(v._1, v._2))) // or
      .map(_.map((BodyParams.apply _).tupled))
  }


  def getById(n: Int): DBIO[Seq[Dinosaur]] = {
    dinosaurs
      .filter(_.id === n)
      .result
  }


  val big = Option(4000.0)

  def findAndUpgrade: Unit  =  {
    dinosaurs
      .filterOpt(big) {
        _.weight > _
      }
      .map(_.weight)
      .update(3000.0)

  }

  def leftJoinExample: DBIO[Seq[(Dinosaur, Option[EventLogs])]] = {
    for {
      (d, e) <- dinosaurs joinLeft eventLogs on (_.id === _.dinosaurId)
    } yield (d, e)
  }.result


  def rightJoinExample: DBIO[Seq[(Option[Dinosaur], EventLogs)]] = {
    for {
      (d, e) <- dinosaurs joinRight eventLogs on (_.id === _.dinosaurId)
    } yield (d, e)
  }.result

  def fullJoinExample: DBIO[Seq[(Option[Dinosaur], Option[EventLogs])]] = {
    for {
      (d, e) <- dinosaurs joinFull eventLogs on (_.id === _.dinosaurId)
    } yield (d, e)
  }.result

  def innerJoinExample: DBIO[Seq[(Dinosaur, EventLogs)]] = {
    for {
      (d, e) <- dinosaurs join eventLogs on (_.id === _.dinosaurId)
    } yield (d, e)
  }.result

  // Fuuuuuuuuuuuu
  def crossJoinExample: DBIO[Seq[(Dinosaur, EventLogs)]] = {
    for {
      (d, e) <- dinosaurs join eventLogs
    } yield (d, e)
  }.result


  def leftJoinFilter: DBIO[Seq[(Dinosaur, EventLogs)]] = {
    for {
      d <- dinosaurs
      e <- eventLogs if e.dinosaurId === d.id
    } yield (d, e)
  }.result

  def leftJoinCase: DBIO[Seq[(Dinosaur, EventLogs)]] = {
    dinosaurs
      .join(eventLogs) on {
      case (d, e) => d.id === e.dinosaurId
    }
  }.result


  def leftJoinTime: DBIO[Seq[TimeOfCreation]] = {
    val res = for {
      (d, e) <- dinosaurs joinLeft eventLogs on (_.id === _.dinosaurId)
    } yield (d, e)

    res.result.map(_.map {
      case (d, e) => TimeOfCreation(d.id, e.map(_.createdOn))
    })
  }


}






