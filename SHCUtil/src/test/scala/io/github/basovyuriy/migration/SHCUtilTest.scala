
package io.github.basovyuriy.migration

import org.scalatest.FlatSpec

class SHCUtilTest extends FlatSpec {
  "SHCUtil" should "parse correctly" in {
    val inputs = SHCUtil.parseParams (SHCUtil.loadSample ())
    assert (inputs.zookeeperQuorum == "os-0001,os-0002,os-0003:2181")
    val expected = "{\"hbase.zookeeper.quorum\":\"os-0001:2181,os-0002:2181,os-0003:2181\"}"
    assert (expected == SHCUtil.formatZkJson (inputs))
  }
}