import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Paths

object Query3 {

  val test: Boolean = false
  case class Person(id: Int, x: Double, y: Double, age: Int, gender: String, infected: String)
  case class Pair(p: Person, cells: Array[(Int, Int)])
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query3")
    val sc = new SparkContext(sparConf)

    // load data and parse into Person objects
    var file_end: String = "-large"
    if(test) {
      file_end = "-test"
    }
    val all: RDD[String] = sc.textFile("data_problem1/PEOPLE-SOME-INFECTED" + file_end + ".txt")
    val all_split : RDD[Array[String]] = all.map(line => line.split(","))
    val all_people : RDD[Person] = all_split.map(x => Person(x(0).toInt, x(1).toDouble, x(2).toDouble, x(3).toInt, x(4), x(5)))

    // get list of cell ids for each person using custom function, then parse into Pair objects
    val all_cellIDs : RDD[(Person, Array[(Int, Int)])] = all_people.map(getCellIDs)
    val all_pairs : RDD[Pair] = all_cellIDs.map(x => Pair(x._1, x._2))

    // using flatmap to create a new row for each cell id in pair
    // also note that the order has flipped so that the cell id (int, int) can now be the key
    val all_split_cells = {
      all_pairs.flatMap { case Pair(p, cells) => cells.map((_,p))}
    }

    // split rdd into infected and not infected rdds
    val infected : RDD[((Int, Int), Person)] = all_split_cells.filter{x => x._2.infected =="yes"}
    val not_infected : RDD[((Int, Int), Person)] = all_split_cells.filter{x => x._2.infected =="no"}

    // inner join the rdds on cell id so that all infected people are matched up with non infected people in the same cells
    // then filter to only keep the close contacts
    val cell_join : RDD[((Int, Int), (Person, Person))] = infected.join(not_infected)
    val close_contacts : RDD[((Int, Int), (Person, Person))] = cell_join.filter {
      x =>
        math.sqrt(math.pow(x._2._1.x - x._2._2.x, 2) + math.pow(x._2._1.y - x._2._2.y, 2)) <= 6.0
    }

    // map new rdd of just infected id, and non-infected id, then get distinct as multiple cell assignments could lead
    // to duplicate entries
    val close_contact_ids : RDD[(Int, Int)] = close_contacts.map(x => (x._2._1.id, x._2._2.id)).distinct()

    // group by key, and map to list.size is like reducing by count
    val contact_counts : RDD[(Int, Int)] = close_contact_ids.groupByKey().map(x => (x._1, x._2.size))

    val resultArray: Array[(Int, Int)] = contact_counts.collect()

    val relPath = Paths.get("results_problem1") + "/output_query3" + file_end
    contact_counts.saveAsTextFile(relPath)

    sc.stop()
  }

  // custom function to get all cell ids for a person
  // cell id is (int, int) indicating the position of the cell they are in
  // default cell size is 100
  def getCellIDs(person: Person) : (Person, Array[(Int, Int)]) = {

    val cellSize: Double = 100
    var cellIDs: Array[(Int, Int)] = new Array[(Int, Int)](0)

    val cellX: Double = person.x / cellSize
    val cellY: Double = person.y / cellSize

    // cell id that person is in but need to check for edge cases
    cellIDs :+= (cellX.toInt, cellY.toInt)

    // add other cell ids depending on the edge case
    val x_edge = person.x % cellSize
    val y_edge = person.y % cellSize

    if (x_edge <= 6) {
      cellIDs :+= (cellX.toInt - 1, cellY.toInt)
      if(y_edge <= 6) {
        cellIDs :+= (cellX.toInt - 1, cellY.toInt - 1)
      } else if (y_edge >= 94) {
        cellIDs :+= (cellX.toInt - 1, cellY.toInt + 1)
      }
    } else if (x_edge >= 94) {
      cellIDs :+= (cellX.toInt + 1, cellY.toInt)
      if(y_edge <= 6) {
        cellIDs :+= (cellX.toInt + 1, cellY.toInt - 1)
      } else if (y_edge >= 94) {
        cellIDs :+= (cellX.toInt + 1, cellY.toInt + 1)
      }
    }
    if(y_edge <= 6) {
      cellIDs :+= (cellX.toInt, cellY.toInt - 1)
    } else if (y_edge >= 94) {
      cellIDs :+= (cellX.toInt, cellY.toInt + 1)
    }

    return (person,cellIDs)
  }
}
