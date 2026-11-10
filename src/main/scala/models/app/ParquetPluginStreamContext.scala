package com.sneaksanddata.arcane.stream_parquet
package models.app

import com.sneaksanddata.arcane.framework.models.app.{
  DefaultPluginStreamContext,
  OverrideStreamContext,
  PluginStreamContext
}
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.DefaultSinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.DefaultStagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultStreamModeSettings,
  DefaultThroughputSettings
}
import upickle.default.*
import upickle.implicits.key
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** The specification for the stream.
  */
case class ParquetPluginStreamContext(
    @key("observability") private val observabilityIn: DefaultObservabilitySettings,
    @key("staging") private val stagingIn: DefaultStagingSettings,
    @key("streamMode") private val streamModeIn: DefaultStreamModeSettings,
    @key("sink") private val sinkIn: DefaultSinkSettings,
    @key("throughput") private val throughputIn: DefaultThroughputSettings,
    override val source: ParquetStreamSourceSettings
) extends DefaultPluginStreamContext(observabilityIn, stagingIn, streamModeIn, sinkIn, throughputIn) derives ReadWriter:
  override def merge[OtherImpl <: OverrideStreamContext](
      other: Option[OtherImpl]
  ): ParquetPluginStreamContext =
    other match
      case None => this
      case Some(o: ParquetOverrideStreamContext) =>
        this.copy(
          observabilityIn = this.observabilityIn.merge(o.observability),
          stagingIn = this.staging.merge(o.staging),
          streamModeIn = this.streamMode.merge(o.streamMode),
          sinkIn = this.sinkIn.merge(o.sink),
          throughputIn = this.throughput.merge(o.throughput),
          source = this.source.merge(o.source)
        )
      case _ => this

object ParquetPluginStreamContext:
  def apply(value: String): ParquetPluginStreamContext = PluginStreamContext[ParquetPluginStreamContext](value)

  lazy val layer
      : ZLayer[Any, Throwable, PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig] =
    PluginStreamContext.getLayer[ParquetPluginStreamContext, ParquetOverrideStreamContext]
