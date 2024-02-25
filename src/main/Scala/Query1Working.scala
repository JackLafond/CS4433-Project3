import scala.io.Source

case class Person(id: Int, x: Double, y: Double, age: Int, gender: String)

object Query1 {

  def main(args: Array[String]): Unit = {
    val peopleFile = "PEOPLE-large-test.txt"
    val infectedFile = "INFECTED-small-test.txt"

    val people = loadPeople(peopleFile)
    val infected = loadPeople(infectedFile)

    val cellSize = 100.0
    val cellMap = partitionIntoCells(people, cellSize)

    val closeContacts = findCloseContacts(infected, cellMap, cellSize)

    closeContacts.foreach(println)
  }

  // Loads people into Person objects from file
  def loadPeople(filename: String): List[Person] = {
    Source.fromFile(filename)
      .getLines()
      .map { line =>
        val Array(id, x, y, age, gender) = line.split(",")
        Person(id.toInt, x.toDouble, y.toDouble, age.toInt, gender)
      }
      .toList
  }

  // Partitions people into cells
  def partitionIntoCells(people: List[Person], cellSize: Double): Map[(Int, Int), List[Person]] = {
    people.groupBy(person => ((person.x / cellSize).toInt, (person.y / cellSize).toInt))
  }

  // Maps infected people to their close contacts
  // Returns a list of tuples (pJ, infectI) where pJ is a close contact of infectI
  // Runs once for each infected person
  def findCloseContacts(infected: List[Person], cellMap: Map[(Int, Int), List[Person]], cellSize: Double): List[(Person, Person)] = {
    infected.flatMap { infectI =>
      val cellX = (infectI.x / cellSize).toInt
      val cellY = (infectI.y / cellSize).toInt

      // neighboringCells is a list of all cells that are within 1 unit of infectI, including the cell infectI is in
      // TODO: only add neighboring cells for which the distance to infectI is less than 6
      val neighboringCells = for {
        offsetX <- -1 to 1
        offsetY <- -1 to 1
      } yield (cellX + offsetX, cellY + offsetY)

      neighboringCells
        .flatMap(cellMap.getOrElse(_, List.empty))
        .filter { pJ =>
          // Check if pJ is within 6 units range of infectI
          math.sqrt(math.pow(infectI.x - pJ.x, 2) + math.pow(infectI.y - pJ.y, 2)) <= 6.0
        }
        .map(pJ => (pJ, infectI))
    }
  }
}
