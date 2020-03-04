import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

sealed abstract class TypeOfFood(val value: String)

object TypeOfFood {

  case object Predator extends TypeOfFood("Predator")

  case object Herbivorous extends TypeOfFood("Herbivorous")

  case object Omnivores extends TypeOfFood("Omnivores")

  def withName(`type`: String): TypeOfFood = allValues.find(v => v.value == `type`).getOrElse(throw new Exception("unexpected TypeOfFood"))

  private val allValues: Seq[TypeOfFood] = Seq(Predator, Herbivorous, Omnivores)

}

final case class Dinosaur(id: Option[Int],
                          weight: Double,
                          height: Double,
                          gender: Boolean,
                          isFly: Boolean,
                          isSwim: Boolean,
                          typeOfFood: TypeOfFood,
                          lifespan: Option[Int])

trait DinosaurModel extends InitTable {

  implicit val typeOfFoodMapped = MappedColumnType.base[TypeOfFood, String](
    in => in.value,
    out => TypeOfFood.withName(out)
  )

  class DinosaurTable(tag: Tag) extends Table[Dinosaur](tag, "dinosaur") {

    def id = column[Option[Int]]("id", O.PrimaryKey)

    def weight = column[Double]("weight")

    def height = column[Double]("height")

    def gender = column[Boolean]("gender")

    def isFly = column[Boolean]("isFly")

    def isSwim = column[Boolean]("isSwim")

    def typeOfFood = column[TypeOfFood]("typeOfFood")

    def lifespan = column[Option[Int]]("lifespan")

    override def * = (id, weight, height, gender, isFly, isSwim, typeOfFood, lifespan).mapTo[Dinosaur]

    // override def * = (id, weight, height, gender, isFly, isSwim, typeOfFood, lifespan) <> ((Dinosaur.apply _).tupled, Dinosaur.unapply)
  }

  val dinosaurs = TableQuery[DinosaurTable]
  lazy val dinosaursSeq: Sequence[Int] = Sequence[Int]("dino_id_seq")

  override def init = dinosaurs.schema.create

}

