package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {

  def _parseLatLon(value: String): (Double, Double) = {
    val parsed = value.split(",").toList.map(_.toDouble)
    (parsed.head, parsed(1))
  }

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

    // parse rectangle
    val rectangleValues = queryRectangle.split(",").toList.map(_.toDouble)
    val rectangleLat1 = rectangleValues.head
    val rectangleLat2 = rectangleValues(2)
    val rectangleLon1 = rectangleValues(1)
    val rectangleLon2 = rectangleValues(3)

    // parse point
    val (pointLat, pointLon) = _parseLatLon(pointString)

    // check if within bounds
    def withinRange(bound1: Double, bound2: Double, value: Double): Boolean = {
      val lower = Math.min(bound1, bound2)
      val upper = Math.max(bound1, bound2)
      value >= lower && value <= upper
    }

    withinRange(rectangleLat1, rectangleLat2, pointLat) && withinRange(rectangleLon1, rectangleLon2, pointLon)
  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val (lat1, lon1) = _parseLatLon(pointString1)
    val (lat2, lon2) = _parseLatLon(pointString2)
    val euclidean_distance = Math.sqrt(Math.pow(lat1 - lat2, 2) + Math.pow(lon1 - lon2, 2))
    euclidean_distance <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Contains", (rectangleString: String, pointString: String) => ST_Contains(rectangleString, pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('" + arg2 + "',point._c0)")
    resultDf.show()

    resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains", (rectangleString: String, pointString: String) => ST_Contains(rectangleString, pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => ST_Within(pointString1, pointString2, distance))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")")
    resultDf.show()

    resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => ST_Within(pointString1, pointString2, distance))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")")
    resultDf.show()

    resultDf.count()
  }
}
