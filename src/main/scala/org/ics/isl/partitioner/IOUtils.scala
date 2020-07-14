package org.ics.isl.partitioner

import java.io.{File, FileWriter, PrintWriter}
import scala.collection.Map
import scala.io.Source

object IOUtils {

  final private val PATH_DELIM = "/"
  final private val FILE_DELIM = ","
  final private val NEW_LINE = "\n"


  /**
   *
   * @param map
   * @param path
   * @tparam K
   * @tparam V
   */
  def writeMap[K, V](map: Map[K, V], path: String): Unit = {
    val dirPath = path.split(PATH_DELIM).dropRight(1).mkString(PATH_DELIM)
    createDirs(dirPath)

    val pw = new PrintWriter(new FileWriter(path, false))
    map.foreach{case (uri, freq) =>
      pw.write(uri + FILE_DELIM + freq + NEW_LINE)
    }
    pw.close()
  }


  /**
   * Creates all directories in the given path
   * @param path
   */
  private def createDirs(path: String): Unit = {
    val dir = new File(path)
    if(!dir.exists())
      dir.mkdirs()
  }


 /**
   *
   * @param path
   * @return
   */
  def loadMap(path: String): Map[String, String] = {
    var m = Map[String, String]()
    val source = Source.fromFile(path)
    for (line <- source.getLines) {
      val tokens: Array[String] = line.split(FILE_DELIM)
      m = m + (tokens(0) -> tokens(1))
    }
    source.close()
    m
  }
}