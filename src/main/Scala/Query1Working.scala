import scala.io.Source

object Query1Working {

  case class Person(id: Int, x: Double, y: Double, age: Int, gender: String)

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

      // minOffsetX, minOffsetY, maxOffsetX, maxOffsetY are used to check if infectI is on the edge of a cell
      var minOffsetX, minOffsetY, maxOffsetX, maxOffsetY = 0

      if (infectI.x % cellSize <= 6) {
        minOffsetX = -1
      }
      else if (infectI.x % cellSize >= cellSize - 6) {
        maxOffsetX = 1
      }
      if (infectI.y % cellSize <= 6) {
        minOffsetY = -1
      }
      else if (infectI.y % cellSize >= cellSize - 6) {
        maxOffsetY = 1
      }

      // neighboringCells is a list of all cells that are within 6 units of infectI, including the cell infectI is in
      val neighboringCells = for {
        offsetX <- minOffsetX to maxOffsetX
        offsetY <- minOffsetY to maxOffsetY
      } yield (cellX + offsetX, cellY + offsetY)

      neighboringCells
        .flatMap(cellMap.getOrElse(_, List.empty))
        .filter { pJ =>
          // Check if pJ is within 6 units range of infectI
          math.sqrt(math.pow(infectI.x - pJ.x, 2) + math.pow(infectI.y - pJ.y, 2)) <= 6.0 &&
          // Do not print pair if infectI = pJ
          infectI != pJ
        }
        .map(pJ => (pJ, infectI))
    }
  }
}
