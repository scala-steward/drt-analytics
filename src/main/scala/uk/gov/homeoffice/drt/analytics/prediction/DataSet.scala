package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, monotonically_increasing_id}
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.{FeatureType, OneToMany, Single}

import scala.collection.immutable

case class DataSet[T](df: DataFrame, columnNames: Seq[String], featureSpecs: List[FeatureType]) {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  val dfIndexed: DataFrame = df.withColumn("_index", monotonically_increasing_id())

  val numRows: Long = dfIndexed.count()

  lazy val lr = new LinearRegression()

  def trainModel(labelCol: String, trainingSplitPercentage: Int)
                (implicit session: SparkSession): LinearRegressionModel = {
    lr.fit(prepare(labelCol, trainingSplitPercentage, sortAscending = true))
  }

  def predict(labelCol: String, predictionSplitPercentage: Int, model: LinearRegressionModel)
             (implicit session: SparkSession): DataFrame =
    model
      .transform(prepare(labelCol, predictionSplitPercentage, sortAscending = false))
      .select(col("index"), col("prediction"))
      .sort(col("index"))

  def prepare(labelColName: String, takePercentage: Int, sortAscending: Boolean)
             (implicit session: SparkSession): DataFrame = {
    import session.implicits._

    val oneToManyFeatures = extractOneToManyFeatures(df)

    val featureColumns = featureSpecs.map {
      case OneToMany(columnNames, _) => concat_ws("-", columnNames.map(col): _*)
      case Single(columnName) => col(columnName)
    }
    val labelAndFeatures = col(labelColName) :: (featureColumns ++ columnNames.map(col))

    val partitionIndexValue = (numRows * (takePercentage.toDouble / 100)).toInt

    val sortBy = if (sortAscending) $"_index".asc else $"_index".desc

    val trainingSet = dfIndexed
      .select(labelAndFeatures: _*)
      .sort(sortBy)
      .limit(partitionIndexValue)
      .collect.toSeq
      .map { row =>
        val oneToManyValues: immutable.Seq[(Int, Double)] = featureSpecs
          .zipWithIndex
          .collect {
            case (otm: OneToMany, idx) => (otm, idx)
          }
          .map {
            case (fs, idx) =>
              val featureString = s"${fs.featurePrefix}${row.getAs[String](idx + 1)}"
              val featureIdx = oneToManyFeatures.indexOf(featureString)
              (featureIdx, 1d)
          }
        val singleValues = featureSpecs.collect {
          case Single(columnName) => row.getAs[Double](columnName)
        }

        val featureValues = Vectors.sparse(oneToManyFeatures.length, oneToManyValues).toArray ++ singleValues

        (row.getAs[Double](0), Vectors.dense(featureValues), row.getAs[String]("index"))
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
