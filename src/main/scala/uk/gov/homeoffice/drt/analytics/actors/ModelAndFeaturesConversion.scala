package uk.gov.homeoffice.drt.analytics.actors

import uk.gov.homeoffice.drt.protobuf.messages.ModelAndFeatures.{FeaturesMessage, ModelAndFeaturesMessage, ModelsAndFeaturesMessage, OneToManyFeatureMessage, RegressionModelMessage}
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, ModelAndFeatures, RegressionModel, TouchdownModelAndFeatures}

object ModelAndFeaturesConversion {
  def modelsAndFeaturesFromMessage(msg: ModelsAndFeaturesMessage): Iterable[ModelAndFeatures] =
    msg.modelsAndFeatures.map(modelAndFeaturesFromMessage)

  def modelAndFeaturesFromMessage(msg: ModelAndFeaturesMessage): ModelAndFeatures = {
    val model = msg.model.map(modelFromMessage).getOrElse(throw new Exception("No value for model"))
    val features = msg.features.map(featuresFromMessage).getOrElse(throw new Exception("No value for features"))
    val targetName = msg.targetName.getOrElse(throw new Exception("Mandatory parameter 'targetName' not specified"))
    val examplesTrainedOn = msg.examplesTrainedOn.getOrElse(throw new Exception("Mandatory parameter 'examplesTrainedOn' not specified"))
    val improvementPct = msg.improvementPct.getOrElse(throw new Exception("Mandatory parameter 'improvement' not specified"))

    ModelAndFeatures(model, features, targetName, examplesTrainedOn, improvementPct)
  }

  def modelFromMessage(msg: RegressionModelMessage): RegressionModel =
    RegressionModel(msg.coefficients, msg.intercept.getOrElse(throw new Exception("No value for intercept")))

  def featuresFromMessage(msg: FeaturesMessage): FeaturesWithOneToManyValues = {
    val singles = msg.singleFeatures.map(Single)
    val oneToManys = msg.oneToManyFeatures.map(oneToManyFromMessage)
    val allFeatures = oneToManys ++ singles

    FeaturesWithOneToManyValues(allFeatures.toList, msg.oneToManyValues.toIndexedSeq)
  }

  def oneToManyFromMessage(msg: OneToManyFeatureMessage): OneToMany =
    OneToMany(msg.columns.toList, msg.prefix.getOrElse(throw new Exception("No value for prefix")))

  def modelToMessage(model: RegressionModel): RegressionModelMessage =
    RegressionModelMessage(
      coefficients = model.coefficients.toArray,
      intercept = Option(model.intercept),
    )

  def featuresToMessage(features: FeaturesWithOneToManyValues): FeaturesMessage = {
    FeaturesMessage(
      oneToManyFeatures = features.features.collect {
        case OneToMany(columnNames, featurePrefix) =>
          OneToManyFeatureMessage(columnNames, Option(featurePrefix))
      },
      singleFeatures = features.features.collect {
        case Single(columnName) => columnName
      },
      oneToManyValues = features.oneToManyValues
    )
  }

  def modelAndFeaturesToMessage(modelAndFeatures: ModelAndFeatures, now: Long): ModelAndFeaturesMessage = {
    ModelAndFeaturesMessage(
      model = Option(modelToMessage(modelAndFeatures.model)),
      features = Option(featuresToMessage(modelAndFeatures.features)),
      targetName = Option(modelAndFeatures.targetName),
      examplesTrainedOn = Option(modelAndFeatures.examplesTrainedOn),
      improvementPct = Option(modelAndFeatures.improvementPct),
      timestamp = Option(now),
    )
  }

  def modelsAndFeaturesToMessage(models: Iterable[ModelAndFeatures], now: Long): ModelsAndFeaturesMessage =
    ModelsAndFeaturesMessage(models.map(modelAndFeaturesToMessage(_, now)).toSeq)
}
