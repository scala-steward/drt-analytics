package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionSummary}
import org.apache.spark.sql.functions.{col, concat_ws, monotonically_increasing_id}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{Feature, FeaturesWithOneToManyValues}

import scala.collection.immutable

case class DataSet(df: DataFrame, features: List[Feature]) {
  val dfIndexed: DataFrame = df.withColumn("_index", monotonically_increasing_id())

  val numRows: Long = dfIndexed.count()
  val oneToManyFeatureValues: IndexedSeq[String] = features.flatMap {
    case _: Single => Iterable()
    case OneToMany(columnNames, featurePrefix) =>
      df
        .select(concat_ws("-", columnNames.map(c => col(c.label)): _*))
        .rdd.distinct.collect
        .map(r => s"${featurePrefix}_${r.getAs[String](0)}")
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

    val labelAndFeatures: Seq[Column] = FeatureVectors.labelAndFeatureCols(df.columns, labelColName, features)

    val partitionIndexValue = (numRows * (takePercentage.toDouble / 100)).toInt

    val sortBy = if (sortAscending) $"_index".asc else $"_index".desc

    dfIndexed
      .select(labelAndFeatures: _*)
      .sort(sortBy)
      .limit(partitionIndexValue)
      .collect.toSeq
      .map { row =>
        val label = row.getAs[Double](0)
        val featuresVector = FeatureVectors.featuresVectorForRow(row, featuresWithOneToManyValues)
        val index = row.getAs[String]("index")
        (label, featuresVector, index)
      }
      .toDF("label", "features", "index")
  }
}
