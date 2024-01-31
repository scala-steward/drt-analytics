package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionSummary}
import org.apache.spark.sql.functions.{col, concat_ws, monotonically_increasing_id}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.prediction.FeaturesWithOneToManyValues
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{Feature, OneToMany, Single}

import scala.util.Try

case class DataSet(df: DataFrame, features: List[Feature[_]]) {
  private val log = LoggerFactory.getLogger(getClass)

  val dfIndexed: DataFrame = df.withColumn("_index", monotonically_increasing_id())

  val numRows: Long = dfIndexed.count()
  val oneToManyFeatureValues: IndexedSeq[String] = features.flatMap {
    case _: Single[_] => Iterable()
    case feature: OneToMany[_] =>
      df
        .select(concat_ws("-", col(feature.label)))
        .rdd.distinct.collect
        .map(_.getAs[String](0))
  }.toIndexedSeq

  val featuresWithOneToManyValues: FeaturesWithOneToManyValues = FeaturesWithOneToManyValues(features, oneToManyFeatureValues)

  def trainModel(labelCol: String, trainingSplitPercentage: Int)
                (implicit session: SparkSession): LinearRegressionModel =
    new LinearRegression()
      .setRegParam(1)
      .fit(prepareDataFrame(labelCol, trainingSplitPercentage, sortAscending = true))

  def evaluate(labelCol: String, trainingSplitPercentage: Int, model: LinearRegressionModel)
              (implicit session: SparkSession): LinearRegressionSummary =
    model.evaluate(prepareDataFrame(labelCol, trainingSplitPercentage, sortAscending = true))

  def predict(labelCol: String, predictionSplitPercentage: Int, model: LinearRegressionModel)
             (implicit session: SparkSession): DataFrame =
    model
      .transform(prepareDataFrame(labelCol, 100 - predictionSplitPercentage, sortAscending = false))
      .sort(col("index"))

  private def prepareDataFrame(labelColName: String, takePercentage: Int, sortAscending: Boolean)
                              (implicit session: SparkSession): DataFrame = {
    import session.implicits._

    val labelAndFeatures = FeatureVectors.labelAndFeatureCols(df.columns, labelColName)

    val partitionIndexValue = (numRows * (takePercentage.toDouble / 100)).toInt

    val sortBy = if (sortAscending) $"_index".asc else $"_index".desc

    dfIndexed
      .select(labelAndFeatures: _*)
      .sort(sortBy)
      .limit(partitionIndexValue)
      .collect.toSeq
      .map { row =>
        val label = row.getAs[Double](0)
        val index = row.getAs[String]("index")
        Try(FeatureVectors.featuresVectorForRow(row, featuresWithOneToManyValues)) match {
          case scala.util.Success(featuresVector) =>
            Option((label, featuresVector, index))
          case scala.util.Failure(t) =>
            log.error(s"Failed to create features vector for row $row. Features: $featuresWithOneToManyValues", t)
            None
        }
      }
      .collect {
        case Some((label, featuresVector, index)) => (label, featuresVector, index)
      }
      .toDF("label", "features", "index")
  }
}
