package com.sneaksanddata.arcane.stream_parquet
package tests

import main.{appLayer, blobSourceLayer, s3ReaderLayer}
import models.app.ParquetPluginStreamContext

import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.blobsource.backfill.{
  BlobBackfillSourceDataProvider,
  BlobShardedBackfillStreamDataProvider,
  BlobSourceBackfillMergeStreamDataProvider,
  BlobSourceShardFactory
}
import com.sneaksanddata.arcane.framework.services.blobsource.providers.{
  BlobSourceDataProvider,
  BlobSourceStreamingDataProvider
}
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetSource
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.UpsertBlobStagedBatchFactory
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.cleanup.CatalogDisposeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.storage.models.s3.{S3ClientSettings, S3StoragePath}
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import com.sneaksanddata.arcane.framework.testkit.streaming.TimeLimitLifetimeService
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import zio.{ULayer, ZIO, ZLayer}

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
      streamContextLayer: ZLayer[Any, Nothing, ParquetPluginStreamContext]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer,
      s3ReaderLayer,
      BlobSourceStreamingDataProvider.layer
    )(
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      DeclaredMetrics.layer,
      GlobalMetricTagProvider.layer,
      WatermarkProcessor.layer,
      ZLayer.succeed(TimeLimitLifetimeService(runDuration)),
      blobSourceLayer,
      DefaultStreamBootstrapper.layer,
      ThroughputShaperBuilder.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergTablePropertyManager.stagingLayer,
      IcebergTablePropertyManager.sinkLayer,
      UpsertBlobStagedBatchFactory.layer,
      BlobSourceDataProvider.layer,

      // backfill
      BlobBackfillSourceDataProvider.layer,
      BlobSourceShardFactory.layer,
      BlobShardedBackfillStreamDataProvider.layer,
      BlobSourceBackfillMergeStreamDataProvider.layer,
      DefaultBackfillStateManager.layer,
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer,

      // schema
      SchemaMigrationProcessor.layer,

      // maintenance and cleanup
      TargetMaintenanceProcessor.layer,
      CatalogDisposeServiceClient.layer,
      DefaultNameGenerator.layer
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
        S3BlobStorageReader(
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
