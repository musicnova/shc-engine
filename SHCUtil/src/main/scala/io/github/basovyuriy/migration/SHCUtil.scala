
package io.github.basovyuriy.migration

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object SHCUtil {

  def runCheckStatusJob (args: Array[String]): Unit = {
    val conf = new SparkConf ()
    conf.set ("spark.dynamicAllocation.enabled", "false")

    val sc = new SparkContext (conf)
    try {
      throw new Exception("checkStatus is not implemented yet")
    } finally {
      sc.stop ()
    }
  }

  def loadConfig (localPath: String): Array[String] = {
    val bufferedSource = Source.fromFile (localPath)
    val res = bufferedSource.getLines ().toList.toArray[String]
    bufferedSource.close ()
    res
  }

  def loadSample (resourcePath: String = "sample.txt"): Array[String] = {
    s"""
       nameNode = hdfs://test-cluster
       resourceManager = os-0001.mydata.com:8032
       oozieRoot = /mydata/oozie
       bundleName = bdl_select_mydata_1
       applicationName = select_mydata_1
       clusterName = TEST
       datalakePath = /mydata/datalake/fix_1
       stagingPath = /mydata/staging/fix_1
       zookeeperQuorum=os-0001,os-0002,os-0003:2181
       kafkaBrokers=os-0001.my.com,os-0002.my.com,os-0003.my.com:9092

       oozie.use.system.libpath = true
       oozie.wf.application.path = ${nameNode}${oozieRoot}/workflow/wf_${applicationName}/wf_${applicationName}.xml
       mapreduce.framework.name = yarn
      """.split("\n")
  }

  def parsePropElem (kv: String, elem: String): String = {
    val kvArr: Array[String] = kv.split ("=")
    if (kvArr (0).matches("^[ ]*" + elem + "[ ]*$")) && kvArr.length > 1) kvArr (1) else ""
  }
}