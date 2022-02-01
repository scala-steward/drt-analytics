package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionSummary}
import org.apache.spark.sql.functions.{col, concat_ws, monotonically_increasing_id}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import uk.gov.homeoffice.drt.prediction.FeatureType.{FeatureType, OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.Features

import scala.collection.immutable

case class DataSet(df: DataFrame, featureSpecs: List[FeatureType]) {
  val dfIndexed: DataFrame = df.withColumn("_index", monotonically_increasing_id())

  val oneToManyFeatures: IndexedSeq[String] = extractOneToManyFeatures(df)

  val numRows: Long = dfIndexed.count()

  val features: Features = {
    val oneToManyValues = featureSpecs.flatMap {
      case _: Single => Iterable()
      case OneToMany(columnNames, featurePrefix) =>
        df
          .select(concat_ws("-", columnNames.map(col): _*))
          .rdd.distinct.collect
          .map(r => s"${featurePrefix}_${r.getAs[String](0)}")
    }.toIndexedSeq
    Features(featureSpecs, oneToManyValues)
  }

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

    val labelAndFeatures: immutable.Seq[Column] = FeatureVectors.labelAndFeatureCols(df.columns, labelColName, features)

    val partitionIndexValue = (numRows * (takePercentage.toDouble / 100)).toInt

    val sortBy = if (sortAscending) $"_index".asc else $"_index".desc

    val trainingSet = dfIndexed
      .select(labelAndFeatures: _*)
      .sort(sortBy)
      .limit(partitionIndexValue)
      .collect.toSeq
      .map { row =>
        val label = row.getAs[Double](0)
        val featuresVector = FeatureVectors.featuresVectorForRow(row, features)
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

object FeatureVectors {
  def featuresVectorForRow(row: Row, features: Features): linalg.Vector =
    Vectors.dense(oneToManyFeaturesIndices(row, features) ++ singleFeaturesVector(row, features))

  def oneToManyFeaturesIndices(row: Row, features: Features): Array[Double] =
    Vectors.sparse(features.oneToManyValues.size, oneToManyIndices(row, features).map(idx => (idx, 1d))).toArray

  def singleFeaturesVector(row: Row, features: Features): List[Double] = features.featureTypes.collect {
    case Single(columnName) => row.getAs[Double](columnName)
  }

  def oneToManyIndices(row: Row, features: Features): Seq[Int] =
    features.oneToManyFeatures
      .map { fs =>
        val featureColValues = fs.columnNames.map(c => row.getAs[String](c))
        val featureString = s"${fs.featurePrefix}_${featureColValues.mkString("-")}"
        features.oneToManyValues.indexOf(featureString)
      }
      .filter(_ >= 0)

  def labelAndFeatureCols(allColumns: Iterable[String], labelColName: String, features: Features)
                         (implicit session: SparkSession): immutable.Seq[Column] = {
    val featureColumns = features.featureTypes.map {
      case OneToMany(columnNames, _) => concat_ws("-", columnNames.map(col): _*)
      case Single(columnName) => col(columnName)
    }
    col(labelColName) :: (featureColumns ++ allColumns.map(col))
  }
}
