package uk.gov.homeoffice.drt.analytics.actors

import server.protobuf.messages.ModelAndFeatures.{FeaturesMessage, ModelAndFeaturesMessage, OneToManyFeatureMessage, RegressionModelMessage}
import uk.gov.homeoffice.drt.analytics.actors.TouchdownPredictionActor.{RegressionModel, ModelAndFeatures}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.{OneToMany, Single}
import uk.gov.homeoffice.drt.analytics.prediction.Features

object ModelAndFeaturesConversion {
  def modelToMessage(model: RegressionModel): RegressionModelMessage =
    RegressionModelMessage(
      coefficients = model.coefficients.toArray,
      intercept = Option(model.intercept),
    )

  def featuresToMessage(features: Features): FeaturesMessage = {
    FeaturesMessage(
      oneToManyFeatures = features.featureTypes.collect {
        case OneToMany(columnNames, featurePrefix) =>
          OneToManyFeatureMessage(columnNames, Option(featurePrefix))
      },
      singleFeatures = features.featureTypes.collect {
        case Single(columnName) => columnName
      },
      oneToManyValues = features.oneToManyValues
    )
  }

  def modelAndFeaturesToMessage(modelAndFeatures: ModelAndFeatures, now: Long): ModelAndFeaturesMessage = {
    ModelAndFeaturesMessage(
      model = Option(modelToMessage(modelAndFeatures.model)),
      features = Option(featuresToMessage(modelAndFeatures.features)),
      timestamp = Option(now),
    )
  }
}
