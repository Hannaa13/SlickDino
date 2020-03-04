import java.time.Instant
import slick.jdbc.PostgresProfile.api._

final case class EventLogs(id: Option[Int],
                           dinosaurId: Int,
                           createdOn: Option[Instant],
                           deletedOn: Option[Instant],
                           updatedOn: Option[Instant])

trait EventLogsModel {
  this: DinosaurModel =>

  class EventLogsTable(tag: Tag) extends Table[EventLogs](tag, "event_logs") {

    def id = column[Option[Int]]("id", O.PrimaryKey, O.AutoInc)

    def dinosaurId = column[Int]("dinosaurId")

    def createdOn = column[Option[Instant]]("createdOn")

    def deletedOn = column[Option[Instant]]("deletedOn")

    def updatedOn = column[Option[Instant]]("updatedOn")

    override def * = (id, dinosaurId, createdOn, deletedOn, updatedOn).mapTo[EventLogs]

    def dinoFK = foreignKey("DINO_LOGS_FK", dinosaurId.?, dinosaurs)(_.id)
  }

  val eventLogs = TableQuery[EventLogsTable]

}