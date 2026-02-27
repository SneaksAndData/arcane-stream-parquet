package com.sneaksanddata.arcane.stream_parquet
package tests

import main.appLayer
import models.{S3Reader, UpsertBlobStreamContext}

import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.blobsource.providers.{
  BlobSourceDataProvider,
  BlobSourceStreamingDataProvider
}
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetSource
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.BlobSourceWatermark
import com.sneaksanddata.arcane.framework.services.blobsource.{
  UpsertBlobBackfillOverwriteBatchFactory,
  UpsertBlobHookManager
}
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.storage.models.s3.{S3ClientSettings, S3StoragePath}
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{
  GenericBackfillStreamingMergeDataProvider,
  GenericBackfillStreamingOverwriteDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{
  GenericGraphBuilderFactory,
  GenericStreamingGraphBuilder
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import com.sneaksanddata.arcane.framework.testkit.streaming.TimeLimitLifetimeService
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import zio.{ULayer, ZIO, ZLayer}

import java.sql.{DriverManager, ResultSet}
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
      streamContextLayer: ZLayer[Any, Nothing, UpsertBlobStreamContext.Environment]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer,
      S3Reader.layer,
      BlobSourceStreamingDataProvider.layer,
      UpsertBlobHookManager.layer,
      UpsertBlobBackfillOverwriteBatchFactory.layer
    )(
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      ZLayer.succeed(MutableSchemaCache()),
      BackfillApplyBatchProcessor.layer,
      GenericBackfillStreamingOverwriteDataProvider.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      GenericStreamingGraphBuilder.backfillSubStreamLayer,
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      WatermarkProcessor.layer,
      BackfillOverwriteWatermarkProcessor.layer,
      IcebergTablePropertyManager.layer,
      ZLayer.succeed(TimeLimitLifetimeService(runDuration)),
      BlobSourceDataProvider.layer,
      BlobListingParquetSource.layer
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
