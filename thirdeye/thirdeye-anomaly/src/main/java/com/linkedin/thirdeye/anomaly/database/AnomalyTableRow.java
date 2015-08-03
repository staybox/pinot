package com.linkedin.thirdeye.anomaly.database;

import java.util.Set;

import com.linkedin.thirdeye.anomaly.api.ResultProperties;

/**
 *
 */
public class AnomalyTableRow {

  /** Unique id for anomaly */
  int id;

  /** Id of the function table entry that produced this anomaly */
  int functionId;

  /** Name of the function that produced the anomaly */
  String functionName;

  /** Description of the function table entry that produced this anomaly */
  String functionDescription;

  /** Collection on which the function was run */
  String collection;

  /** The window of time the anomaly occurred at*/
  long timeWindow;

  /** The number of dimensions that are non '*' */
  int nonStarCount;

  /** The dimension key that produced this anomaly as a json string */
  String dimensions;

  /** The estimated proportion that the dimension key contributes to the metric total */
  double dimensionsContribution;

  /** The metrics analyzed by the function */
  Set<String> metrics;

  /** Function defined anomaly score */
  double anomalyScore;

  /** Function defined anomaly volume */
  double anomalyVolume;

  /** FunctionProperties emitted by the AnomalyDetectionFunction */
  ResultProperties properties;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getFunctionId() {
    return functionId;
  }

  public void setFunctionId(int functionId) {
    this.functionId = functionId;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionDescription() {
    return functionDescription;
  }

  public void setFunctionDescription(String functionDescription) {
    this.functionDescription = functionDescription;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public long getTimeWindow() {
    return timeWindow;
  }

  public void setTimeWindow(long timeWindow) {
    this.timeWindow = timeWindow;
  }

  public int getNonStarCount() {
    return nonStarCount;
  }

  public void setNonStarCount(int nonStarCount) {
    this.nonStarCount = nonStarCount;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public double getAnomalyScore() {
    return anomalyScore;
  }

  public void setAnomalyScore(double anomalyScore) {
    this.anomalyScore = anomalyScore;
  }

  public double getAnomalyVolume() {
    return anomalyVolume;
  }

  public void setAnomalyVolume(double anomalyVolume) {
    this.anomalyVolume = anomalyVolume;
  }

  public ResultProperties getProperties() {
    return properties;
  }

  public void setProperties(ResultProperties properties) {
    this.properties = properties;
  }

  public Set<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(Set<String> metrics) {
    this.metrics = metrics;
  }

  public double getDimensionsContribution() {
    return dimensionsContribution;
  }

  public void setDimensionsContribution(double dimensionsContribution) {
    this.dimensionsContribution = dimensionsContribution;
  }
}
