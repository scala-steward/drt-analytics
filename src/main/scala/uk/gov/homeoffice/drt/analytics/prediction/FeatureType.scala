package uk.gov.homeoffice.drt.analytics.prediction

object FeatureType {
  sealed trait FeatureType

  case class Single(columnName: String) extends FeatureType

  case class OneToMany(columnNames: List[String], featurePrefix: String) extends FeatureType
}
