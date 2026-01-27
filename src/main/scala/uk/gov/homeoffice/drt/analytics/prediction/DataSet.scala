package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionSummary}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import uk.gov.homeoffice.drt.prediction.FeaturesWithOneToManyValues
import uk.gov.homeoffice.drt.prediction.arrival.features.{Feature, OneToManyFeature, SingleFeature}

case class DataSet(df: DataFrame, features: List[Feature[_]]) {
  val dfIndexed: DataFrame = df.withColumn("_index", monotonically_increasing_id())

  val numRows: Long = dfIndexed.count()
  val oneToManyFeatureValues: IndexedSeq[String] = features.flatMap {
    case _: SingleFeature[_] => Iterable()
    case feature: OneToManyFeature[_] =>
      df
        .select(concat_ws("-", col(feature.label)))
        .rdd.distinct.collect
        .map(_.getAs[String](0))
  }.toIndexedSeq

  val featuresWithOneToManyValues: FeaturesWithOneToManyValues = FeaturesWithOneToManyValues(features, oneToManyFeatureValues)

  def trainModel(labelCol: String, trainingSplitPercentage: Int)
                (implicit session: SparkSession): LinearRegressionModel =
    new LinearRegression()
      .setRegParam(0.1)
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

    // Broadcast the feature metadata
    val featuresBc = session.sparkContext.broadcast(featuresWithOneToManyValues)

    val createFeaturesVector = udf { (row: Row) =>
      FeatureVectors.featuresVectorForRow(row, featuresBc.value)
    }

    val labelAndFeatures = FeatureVectors.labelAndFeatureCols(df.columns, labelColName)
    val partitionIndexValue = (numRows * (takePercentage.toDouble / 100)).toInt

    dfIndexed
      .select(labelAndFeatures: _*)
      .limit(partitionIndexValue)
      .withColumn("features", createFeaturesVector(struct(col("*"))))
      .select(
        col(labelColName).as("label"),
        col("features"),
        col("index")
      )
      .na.drop()  // filter out nulls from failed vector creation
  }

  def shuffle(): DataSet = copy(df = dfIndexed.sort(rand))
}
