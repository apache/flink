package org.apache.flink.mesos.conf

import java.net.InetSocketAddress

import org.rogach.scallop.ScallopConf

import scala.concurrent.duration._

// TODO: Figure out a way to do HA (leader election) using only mesos primitives.
trait ZookeeperConf extends ScallopConf {

  private val userAndPass = """[^/@]+"""
  private val hostAndPort = """[A-z0-9-.]+(?::\d+)?"""
  private val zkNode = """[^/]+"""
  private val zkURLPattern = s"""^zk://(?:$userAndPass@)?($hostAndPort(?:,$hostAndPort)*)(/$zkNode(?:/$zkNode)*)$$""".r

  lazy val zooKeeperTimeout = opt[Long]("zk_timeout",
    descr = "The timeout for ZooKeeper in milliseconds",
    default = Some(10000L))

  lazy val zooKeeperSessionTimeout = opt[Long]("zk_session_timeout",
    descr = "The timeout for zookeeper sessions in milliseconds",
    default = Some(30 * 60 * 1000L) //30 minutes
  )

  lazy val zooKeeperUrl = opt[String]("zk",
    descr = "ZooKeeper URL for storing state. Format: zk://host1:port1,host2:port2,.../path",
    validate = (in) => zkURLPattern.pattern.matcher(in).matches(),
    default = Some("zk://localhost:2181/marathon")
  )

  def zooKeeperStatePath: String = "%s/state".format(zkPath)

  def zooKeeperLeaderPath: String = "%s/leader".format(zkPath)

  def zooKeeperServerSetPath: String = "%s/apps".format(zkPath)

  def zooKeeperHostAddresses: Seq[InetSocketAddress] =
    for (s <- zkHosts.split(",")) yield {
      val splits = s.split(":")
      require(splits.length == 2, "expected host:port for zk servers")
      new InetSocketAddress(splits(0), splits(1).toInt)
    }

  def zkURL: String = zooKeeperUrl.get.get

  lazy val zkHosts = zkURL match {
    case zkURLPattern(server, _) => server
  }
  lazy val zkPath = zkURL match {
    case zkURLPattern(_, path) => path
  }
  lazy val zkTimeoutDuration = Duration(zooKeeperTimeout(), MILLISECONDS)
  lazy val zkSessionTimeoutDuration = Duration(zooKeeperSessionTimeout(), MILLISECONDS)
}