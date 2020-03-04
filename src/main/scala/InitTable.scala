import slick.dbio.DBIO

trait InitTable {

  def init: DBIO[Unit]
}
