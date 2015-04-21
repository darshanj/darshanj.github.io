package com.thoughtworks.mcs

//import java.sql.Date

import org.apache.spark.sql.{Column, DataFrame, SQLContext}

object McsDataFrame {

  class MCSResults {
    var all = scala.collection.mutable.Map[String,MCSFluentApi]()

    def insert(name: String, df: MCSFluentApi) = {
      all(name) = df
    }
  }

  class MCSFluentApi(var sparkDataFrames: DataFrame, sqlContext: SQLContext) {

    def filter(filterInput: Column): MCSFluentApi = {
      new MCSFluentApi(sparkDataFrames.filter(filterInput), sqlContext)
    }

    def data(columnName: String) = {
      sparkDataFrames(columnName)
    }

    def show() = {
      sparkDataFrames.show()
      this
    }

    def union(dataFrame: MCSFluentApi): MCSFluentApi = {
      new MCSFluentApi(sparkDataFrames.unionAll(dataFrame.sparkDataFrames).distinct, sqlContext)
    }

    def addColumn(newColumnName: String, dataType: String, newColumn: Column): MCSFluentApi = {
      new MCSFluentApi(sparkDataFrames.withColumn(newColumnName, newColumn), sqlContext)
    }

    def joinInner = join("inner", MCSConfiguration.getOutputSequenceFor(0)) _
    def joinLeft = join("left_outer", MCSConfiguration.getOutputSequenceFor(1)) _

    private def as(alias: String) = new MCSFluentApi(sparkDataFrames.as(alias), sqlContext)

    private def join(join_type: String, outCols: Seq[Column])(columnLeft: Column, rightTable: MCSFluentApi, columnRight: Column) = {
      new MCSFluentApi(
        sparkDataFrames
          .as("a")
          .join(rightTable.as("b").sparkDataFrames, columnLeft === columnRight, join_type)
          .select(outCols: _*), sqlContext)
    }
  }

}
