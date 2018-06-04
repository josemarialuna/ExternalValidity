package es.us.spark.mllib.clustering.validation

/**
  * Created by Josem on 27/09/2017.
  */
class MaxRow {
  private var _name: String = ""
  private var _value: Double = 0

  def name = _name

  def value = _value

  def this(name: String, value: Double) {
    this()
    this._name = name
    this._value = value
  }


}