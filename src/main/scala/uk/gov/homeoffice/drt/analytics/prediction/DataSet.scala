package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionSummary}
import org.apache.spark.sql.functions.{col, concat_ws, monotonically_increasing_id}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.{FeatureType, OneToMany, Single}

import scala.collection.immutable

case class DataSet(df: DataFrame, featureSpecs: List[FeatureType]) {
  val dfIndexed: DataFrame = df.withColumn("_index", monotonically_increasing_id())

  val oneToManyFeatures: IndexedSeq[String] = extractOneToManyFeatures(df)

  val numRows: Long = dfIndexed.count()

  val features: Features = Features(df, featureSpecs)

  lazy val lr = new LinearRegression()

  def trainModel(labelCol: String, trainingSplitPercentage: Int)
                (implicit session: SparkSession): LinearRegressionModel =
    lr
      .setRegParam(1)
      .fit(prepare(labelCol, trainingSplitPercentage, sortAscending = true))

  def evaluate(labelCol: String, trainingSplitPercentage: Int, model: LinearRegressionModel)
              (implicit session: SparkSession): LinearRegressionSummary =
    model.evaluate(prepare(labelCol, trainingSplitPercentage, sortAscending = true))

  def predict(labelCol: String, predictionSplitPercentage: Int, model: LinearRegressionModel)
             (implicit session: SparkSession): DataFrame =
    model
      .transform(prepare(labelCol, 100 - predictionSplitPercentage, sortAscending = false))
      .sort(col("index"))

  def prepare(labelColName: String, takePercentage: Int, sortAscending: Boolean)
             (implicit session: SparkSession): DataFrame = {
    import session.implicits._

    val labelAndFeatures: immutable.Seq[Column] = features.labelAndFeatureCols(df.columns, labelColName)

    val partitionIndexValue = (numRows * (takePercentage.toDouble / 100)).toInt

    val sortBy = if (sortAscending) $"_index".asc else $"_index".desc

    val trainingSet = dfIndexed
      .select(labelAndFeatures: _*)
      .sort(sortBy)
      .limit(partitionIndexValue)
      .collect.toSeq
      .map { row =>
        val label = row.getAs[Double](0)
        val featuresVector = features.featuresVectorForRow(row)
        val index = row.getAs[String]("index")
        (label, featuresVector, index)
      }
      .toDF("label", "features", "index")

    trainingSet
  }

  def extractOneToManyFeatures(df: DataFrame): IndexedSeq[String] =
    featureSpecs.flatMap {
      case _: Single => Iterable()
      case OneToMany(columnNames, featurePrefix) =>
        df
          .select(concat_ws("-", columnNames.map(col): _*))
          .rdd.distinct.collect
          .map(r => featurePrefix + r.getAs[String](0))
    }.toIndexedSeq
}
