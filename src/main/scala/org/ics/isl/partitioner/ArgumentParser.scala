package org.ics.isl.partitioner

import scopt.OptionParser

object ArgumentParser {

  case class Arguments(
                      partitionerMode: Int = 0,
                      numPartitions: Int = 1,
                      datasetName: String = "",
                      hdfsPath: String = "",
                      instancesPath: String = "",
                      schemaPath: String = ""
                      )

  def init(): OptionParser[Arguments] = {
    new OptionParser[Arguments]("Arguments") {
      head("LAWA Partitioner")

      opt[Int]('m', "partitionerMode")
        .required()
        .action((x, c) => c.copy(partitionerMode = x))
        .text("partitionerMode is a required file property")
        .text("0 -> LAP, 1 -> BLAP")
        .text("Default value 0")

      opt[Int]('p', "numPartitions")
        .required()
        .action((x, c) => c.copy(numPartitions = x))
        .text("numPartitions is a required file property")
        .text("number of partitions")

      opt[String]('d', "datasetName")
        .required()
        .action((x, c) => c.copy(datasetName = x))
        .text("datasetName is a required file property")
        .text("name of dataset")

      opt[String]('h', "hdfsPath")
        .required()
        .action((x, c) => c.copy(hdfsPath = x))
        .text("hdfsPath is a required file property")
        .text("base path of hdfs")

      opt[String]('i', "instancesPath")
        .required()
        .action((x, c) => c.copy(instancesPath = x))
        .text("instancesPath is a required file property")
        .text("hdfs path of rdf instances")

      opt[String]('s', "schemaPath")
        .required()
        .action((x, c) => c.copy(schemaPath = x))
        .text("schemaPath is a required file property")
        .text("hdfs path of rdf schema")
    }
  }

}
