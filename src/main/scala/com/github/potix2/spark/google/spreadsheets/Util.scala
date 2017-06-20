package com.github.potix2.spark.google.spreadsheets

import com.google.api.services.sheets.v4.model.{ExtendedValue, CellData, RowData}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.JavaConverters._

object Util {
  def convert(schema: StructType, row: Row): Map[String, Object] =
    schema.iterator.zipWithIndex.map { case (f, i) => f.name -> row(i).asInstanceOf[AnyRef] }.toMap

  def toRowData(row: Row): RowData =
      new RowData().setValues(
        row.schema.fields.zipWithIndex.map { case (f, i) =>
          new CellData()
            .setUserEnteredValue(
              f.dataType match {
                case DataTypes.StringType => new ExtendedValue().setStringValue(row.getString(i))
                case DataTypes.LongType => new ExtendedValue().setNumberValue(longVal(row, i))
                case DataTypes.IntegerType => new ExtendedValue().setNumberValue(intVal(row, i))
                case DataTypes.FloatType => new ExtendedValue().setNumberValue(floatVal(row, i))
                case DataTypes.BooleanType => new ExtendedValue().setBoolValue(booleanVal(row, i))
                case DataTypes.DateType => new ExtendedValue().setStringValue(dateVal(row, i))
                case DataTypes.ShortType => new ExtendedValue().setNumberValue(shortVal(row, i))
                case DataTypes.TimestampType => new ExtendedValue().setStringValue(timestampVal(row, i))
              }
            )
        }.toList.asJava
      )

  private def intVal(row: Row, i: Int): java.lang.Double =
    if (row.isNullAt(i)) null else row.getInt(i).toDouble

  private def floatVal(row: Row, i: Int): java.lang.Double =
    if (row.isNullAt(i)) null else row.getFloat(i).toDouble

  private def longVal(row: Row, i: Int): java.lang.Double =
    if (row.isNullAt(i)) null else row.getLong(i).toDouble

  private def shortVal(row: Row, i: Int): java.lang.Double =
    if (row.isNullAt(i)) null else row.getShort(i).toDouble

  private def booleanVal(row: Row, i: Int): java.lang.Boolean =
    if (row.isNullAt(i)) null else row.getBoolean(i)

  private def dateVal(row: Row, i: Int): String =
    if (row.isNullAt(i)) null else row.getDate(i).toString

  private def timestampVal(row: Row, i: Int): String =
    if (row.isNullAt(i)) null else row.getTimestamp(i).toString
}
