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
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergS3CatalogWriter
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
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import zio.{ULayer, ZIO, ZLayer}

import java.sql.{DriverManager, ResultSet}
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  type StreamLifeTimeServiceLayer = ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken]
  type StreamContextLayer         = ULayer[UpsertBlobStreamContext]

  /** Builds the test application from the provided layers.
    * @param lifetimeService
    *   The lifetime service layer.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def buildTestApp(
      lifetimeService: StreamLifeTimeServiceLayer,
      streamContextLayer: StreamContextLayer
  ): ZIO[Any, Throwable, Unit] =
    appLayer.provide(
      streamContextLayer,
      lifetimeService,
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      S3Reader.layer,
      BlobSourceDataProvider.layer,
      BlobListingParquetSource.layer,
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      BlobSourceStreamingDataProvider.layer,
      UpsertBlobHookManager.layer,
      ZLayer.succeed(MutableSchemaCache()),
      BackfillApplyBatchProcessor.layer,
      GenericBackfillStreamingOverwriteDataProvider.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      GenericStreamingGraphBuilder.backfillSubStreamLayer,
      UpsertBlobBackfillOverwriteBatchFactory.layer,
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      WatermarkProcessor.layer,
      BackfillOverwriteWatermarkProcessor.layer
    )

  /** Gets the data from the *target* table. Using the connection string provided in the
    * `ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI` environment variable.
    *
    * @param targetTableName
    *   The name of the target table.
    * @param decoder
    *   The decoder for the result set.
    * @tparam Result
    *   The type of the result.
    * @return
    *   A ZIO effect that gets the data.
    */
  def getData[Result](
      targetTableName: String,
      columnList: String,
      decoder: ResultSet => Result
  ): ZIO[Any, Throwable, List[Result]] = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet <- ZIO.fromAutoCloseable(
        ZIO.attempt(statement.executeQuery(s"SELECT $columnList from $targetTableName"))
      )
      data <- ZIO.attempt {
        Iterator
          .continually((resultSet.next(), resultSet))
          .takeWhile(_._1)
          .map { case (_, rs) => decoder(rs) }
          .toList
      }
    yield data
  }

  def waitForData[T](
      tableName: String,
      columnList: String,
      decoder: ResultSet => T,
      expectedSize: Int
  ): ZIO[Any, Nothing, Unit] = ZIO
    .sleep(Duration.ofSeconds(1))
    .repeatUntilZIO(_ =>
      (for {
        _ <- ZIO.log("Waiting for data to be loaded")
        inserted <- Common.getData(
          tableName,
          columnList,
          decoder
        )
      } yield inserted.length == expectedSize).orElseSucceed(false)
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

  def getWatermark(targetTableName: String): ZIO[Any, Throwable, BlobSourceWatermark] = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet <- ZIO.fromAutoCloseable(
        ZIO.attemptBlocking(
          statement.executeQuery(
            s"SELECT value FROM iceberg.test.\"$targetTableName$$properties\" WHERE key = 'comment'"
          )
        )
      )
      _         <- ZIO.attemptBlocking(resultSet.next())
      watermark <- ZIO.attempt(BlobSourceWatermark.fromJson(resultSet.getString("value")))
    yield watermark
  }

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
