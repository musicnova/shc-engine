
package io.github.basovyuriy

// import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil

// import org.apache.spark.rdd.RDD
// import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory

// import org.apache.hadoop.hbase.client.Result

import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.FilterList
import org.apache.hadoop.hbase.client.SingleColumnValueFilter
import org.apache.hadoop.hbase.client.NullComparator
import org.apache.hadoop.hbase.client.CompareFilter.CompareOp

// import org.apache.hadoop.hbase.mapreduce.TableInputFormat
// import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.collection.JavaConversions.iterableAsScalaIterable

package object migration {
  case class Inputs (nameNode: String, resourceManager: String, oozieRoot: String
                     , datalakePath: String, stagingPath: String, zookeeperQuorum: String, kafkaBrokers: String) {}

  object HbaseEngineUtil {
    def open (hadoopConfiguration: Configuration): Connection = {
      val connection = ConnectionFactory.createConnection (hadoopConfiguration)
      connection
    }

    def scan (connection: Connection, table: String, parallels: Int
             , qualifiers: Seq[(String, String)]): List[ResultScanner] = {
      val hbaseTable = connection.getTable (TableName.valueOf (table))
      val colsFilter = new FilterList (FilterLost.Operator.MUST_PASS_ONE)
      qualifiers.foreach { cfQ =>
        val filter = new SingleColumnValueFilter (Bytes.toBytes (cfQ._1), Bytes.toBytes(cfQ._2), CompareOp.NOT_EQUAL
        , new NullComparator())
        filter.setFilterIfMissing (true)
        colsFilter.addFilter (filter)
      }
      val aggregatedFilter = new FilterList (FilterList.Operator.MUST_PASS_ALL, colsFilter)
      val scan = new Scan ()
      qualifiers.foreach { cfQ =>
        scan.addColumn (Bytes.toBytes (cfQ._1), Bytes.toBytes (cfQ._2))
      }
      scan.setFilter (aggregatedFilter)
      List (hbaseTable.getScanner (scan))
    }

    def fetch (sc. SparkContext, sq: SQLContext, cursors: List[ResultScanner], limit: Int
      , qualifiers: List[(String, String)]
    , hadoopConfiguration: Configuration: Iterator[DataFrame]) = {
      val keys = qualifiers.map (cfQ => cfQ._1 + "___" + cfQ._2) ++ List ("rowkey")
      val iters = cursots.map (cursor => iterableAsScalaIterable (cursor).map { sr =>
        iterableAsScalaIterable (sr.listCells ()).map(c => {
          val colFamily: String = Bytes.toString (CellUtil.cloneFamily (c))
          val colName: String = Bytes.toString (CellUtil.cloneQualifier (c))
          val colValue: String = Bytes.toString (CellUtil.cloneValue (c))
          val colRow: String = Bytes.toString (CellUtil.cloneRow (c))
          (colFamily + "___" + colName, colValue, colRow)
      })
    }).toIterator


      val defaults = keys.toStream.map (k => (k, "")).toMap
      val schema: StructType = StructType (keys.map (k => StructField (l, StringType, nullable = true)))

      iters.map (parallel => parallel.grouped (limit)).map (grp => {
        val data = grp.map { block => {
          val res = block.flatMap (it => {
            val rowMap = it.flatMap (t => List ( (t._1, t._2), ("rowkey", t._3))).toMap
            defaults.map {case (k, v) => k -> rowMap.getOrElse (k, v) }
          }).toMap
          Row (keys.map { k => res(k) }:_*)
        }}
        sq.createDataFrame (sc.makeRDD (data.toList), schema)
      })
    }
  }
}
