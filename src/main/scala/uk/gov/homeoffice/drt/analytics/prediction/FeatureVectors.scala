package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Row}
import uk.gov.homeoffice.drt.prediction.FeaturesWithOneToManyValues
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{Feature, Single}

import scala.collection.immutable

object FeatureVectors {
  def featuresVectorForRow(row: Row, features: FeaturesWithOneToManyValues): linalg.Vector =
    Vectors.dense(oneToManyFeaturesIndices(row, features) ++ singleFeaturesVector(row, features))

  def oneToManyFeaturesIndices(row: Row, features: FeaturesWithOneToManyValues): Array[Double] =
    Vectors.sparse(features.oneToManyValues.size, oneToManyIndices(row, features).map(idx => (idx, 1d))).toArray

  def singleFeaturesVector(row: Row, features: FeaturesWithOneToManyValues): List[Double] = features.features.collect {
    case feature: Single[_] => row.getAs[Double](feature.label)
  }

  def oneToManyIndices(row: Row, features: FeaturesWithOneToManyValues): Seq[Int] =
    features.oneToManyFeatures
      .map { fs =>
        val featureValue = row.getAs[String](fs.label)
        features.oneToManyValues.indexOf(featureValue)
      }
      .filter(_ >= 0)

  def labelAndFeatureCols(allColumns: Iterable[String],
                          labelColName: String,
                          features: List[Feature[_]]
                         ): immutable.Seq[Column] = {
    val featureColumns = features.map(ft => col(ft.label))
    col(labelColName) :: (featureColumns ++ allColumns.map(col))
  }
}
