package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{Column, Row, SparkSession}
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{Feature, FeaturesWithOneToManyValues}

import scala.collection.immutable

object FeatureVectors {
  def featuresVectorForRow(row: Row, features: FeaturesWithOneToManyValues): linalg.Vector =
    Vectors.dense(oneToManyFeaturesIndices(row, features) ++ singleFeaturesVector(row, features))

  def oneToManyFeaturesIndices(row: Row, features: FeaturesWithOneToManyValues): Array[Double] =
    Vectors.sparse(features.oneToManyValues.size, oneToManyIndices(row, features).map(idx => (idx, 1d))).toArray

  def singleFeaturesVector(row: Row, features: FeaturesWithOneToManyValues): List[Double] = features.features.collect {
    case Single(columnName) => row.getAs[Double](columnName.label)
  }

  def oneToManyIndices(row: Row, features: FeaturesWithOneToManyValues): Seq[Int] =
    features.oneToManyFeatures
      .map { fs =>
        val featureColValues = fs.columns.map(c => row.getAs[String](c.label))
        val featureString = s"${fs.featurePrefix}_${featureColValues.mkString("-")}"
        features.oneToManyValues.indexOf(featureString)
      }
      .filter(_ >= 0)

  def labelAndFeatureCols(allColumns: Iterable[String], labelColName: String, features: List[Feature])
                         (implicit session: SparkSession): immutable.Seq[Column] = {
    val featureColumns = features.map {
      case OneToMany(columns, _) => concat_ws("-", columns.map(c => col(c.label)): _*)
      case Single(column) => col(column.label)
    }
    col(labelColName) :: (featureColumns ++ allColumns.map(col))
  }
}
