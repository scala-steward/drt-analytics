package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, Row}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.{FeatureType, OneToMany, Single}

import scala.collection.immutable

case class Features(featureSpecs: List[FeatureType], oneToManyValues: IndexedSeq[String]) {
  def oneToManyIndices(row: Row): immutable.Seq[Int] =
    featureSpecs
      .collect {
        case otm: OneToMany => otm
      }
      .map { fs =>
        val featureColValues = fs.columnNames.map(c => row.getAs[String](c))
        val featureString = s"${fs.featurePrefix}_${featureColValues.mkString("-")}"
//        println(s"looking for $featureString")
        oneToManyValues.indexOf(featureString)
      }
      .filter(_ >= 0)

  def oneToManyFeaturesIndices(row: Row): Array[Double] =
    Vectors.sparse(oneToManyValues.size, oneToManyIndices(row).map(idx => (idx, 1d))).toArray

  def singleFeaturesVector(row: Row): List[Double] = featureSpecs.collect {
    case Single(columnName) => row.getAs[Double](columnName)
  }

  def featuresVectorForRow(row: Row): linalg.Vector =
    Vectors.dense(oneToManyFeaturesIndices(row) ++ singleFeaturesVector(row))
}

object Features {
  def apply(df: DataFrame, featureSpecs: List[FeatureType]): Features = {
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
}
