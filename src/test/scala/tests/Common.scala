package com.sneaksanddata.arcane.stream_parquet
package tests

import main.{appLayer, blobSourceLayer, s3ReaderLayer}
import models.app.ParquetPluginStreamContext

import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.parquets3.Services
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetStreamingSource
import com.sneaksanddata.arcane.framework.services.storage.models.s3.{S3ClientSettings, S3StoragePath}
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageService
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.{ZIO, ZLayer}

import java.sql.ResultSet
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  /** Builds the test application from the provided layers.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def getTestApp(
      runDuration: Duration,
      streamContextLayer: ZLayer[
        Any,
        Nothing,
        ParquetPluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig
      ]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer,
      s3ReaderLayer
    )(
      blobSourceLayer,
      Services.sourceLayer,
      LayerAssemblies.frameworkPipelineServicesLayer,
      LayerAssemblies.frameworkStagingServicesLayer,
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer
    )

  val TargetDecoder: ResultSet => (Long, String, Long, String, Long, String, Long, String, Long, String, String, Long) =
    (rs: ResultSet) =>
      (
        rs.getLong(1),
        rs.getString(2),
        rs.getLong(3),
        rs.getString(4),
        rs.getLong(5),
        rs.getString(6),
        rs.getLong(7),
        rs.getString(8),
        rs.getLong(9),
        rs.getString(10),
        rs.getString(11),
        rs.getLong(12)
      )

  def getLatestVersion: ZIO[Any, Throwable, Long] =
    for
      reader <- ZIO.succeed(
        S3BlobStorageService(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("minioadmin", "minioadmin")),
          Some(
            S3ClientSettings(
              region = Some("us-east-1"),
              endpoint = Some("http://localhost:9000"),
              pathStyleAccess = true,
              maxResultsPerPage = 1000
            )
          )
        )
      )
      latestFile <- reader
        .streamPrefixes(S3StoragePath("s3a://s3-blob-reader").get)
        .runCollect
        .map(_.maxBy(_.createdOn.getOrElse(0L)))
    yield latestFile.createdOn.getOrElse(0)
