package com.thoughtworks.mcs

import com.thoughtworks.mcs.McsDataFrame.MCSResults
import org.apache.spark.sql.SQLContext

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.Results.Result

class MCS {

  def run(dsl: String, sqlContext: SQLContext): Result = {
    val settings = new Settings()
    settings.usejavacp.value = true
    val interpreter = new IMain(settings)

    val results = new MCSResults

    interpreter.bind("sqlContext", sqlContext)
    interpreter.bind("results", results)

    interpreter.interpret(
      """
          import com.thoughtworks.mcs.operations._
          import com.thoughtworks.mcs.implicits._
          import com.thoughtworks.mcs.McsDataFrame._
          import org.apache.spark.sql.Column
          implicit def RichColumn(column: Column): RichColumn = new RichColumn(column)
      """
    )

    val result = interpreter.interpret(dsl)
    interpreter.reset
    result
  }
}
