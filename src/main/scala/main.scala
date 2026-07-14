package com.sneaksanddata.arcane.stream_parquet

import models.app.ParquetPluginStreamContext

import com.sneaksanddata.arcane.framework.exceptions.StreamFailException
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{
  GenericStreamRunnerService,
  PosixStreamLifetimeService,
  StreamGraphResolver
}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.blobsource.DefaultS3Service
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
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetStreamingSource
import com.sneaksanddata.arcane.framework.services.blobsource.versioning.UpsertBlobStagedBatchFactory
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergSinkEntityManager,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.cleanup.CatalogDisposeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{DataDog, DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.naming.{DefaultNameGenerator, NameGenerator}
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3StoragePath
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageService
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
import zio.*
import zio.logging.backend.SLF4J

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val blobSourceLayer
      : ZLayer[S3BlobStorageService & NameGenerator & PluginStreamContext, Throwable, BlobListingParquetStreamingSource[
        S3StoragePath
      ]] =
    BlobListingParquetStreamingSource.getS3Layer(context =>
      context.asInstanceOf[ParquetPluginStreamContext].source.configuration
    )
  val s3ReaderLayer: ZLayer[PluginStreamContext, Nothing, S3BlobStorageService] =
    DefaultS3Service.getLayer(context => context.asInstanceOf[ParquetPluginStreamContext].source.configuration)

  private def getExitCode(exception: Throwable): zio.ExitCode =
    exception match
      case _: StreamFailException => zio.ExitCode(2)
      case _                      => zio.ExitCode(1)

  private lazy val streamRunner = appLayer.provide(
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer,
    DisposeBatchProcessor.layer,
    FieldFilteringTransformer.layer,
    MergeBatchProcessor.layer,
    StagingProcessor.layer,
    FieldsFilteringService.layer,
    PosixStreamLifetimeService.layer,
    BlobSourceStreamingDataProvider.layer,

    // storage implementation
    s3ReaderLayer,

    // streaming
    blobSourceLayer,
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
    DefaultNameGenerator.layer,
    ParquetPluginStreamContext.layer,
    IcebergS3CatalogWriter.layer,
    IcebergEntityManager.sinkLayer,
    IcebergEntityManager.stagingLayer,
    IcebergTablePropertyManager.stagingLayer,
    IcebergTablePropertyManager.sinkLayer,
    JdbcMergeServiceClient.layer,
    DeclaredMetrics.layer,
    GlobalMetricTagProvider.layer,
    DataDog.UdsPublisher.layer,
    WatermarkProcessor.layer,
    DefaultStreamBootstrapper.layer,
    ThroughputShaperBuilder.layer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = streamRunner

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(getExitCode(cause.squashTrace))
      } yield ()
    }
}
