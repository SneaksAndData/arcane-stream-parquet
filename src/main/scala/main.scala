package com.sneaksanddata.arcane.stream_parquet

import models.app.ParquetPluginStreamContext

import com.sneaksanddata.arcane.framework.exceptions.StreamFailException
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.blobsource.providers.{BlobSourceDataProvider, BlobSourceStreamingDataProvider}
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetSource
import com.sneaksanddata.arcane.framework.services.blobsource.{DefaultS3Reader, UpsertBlobBackfillOverwriteBatchFactory, UpsertBlobHookManager}
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter, IcebergSinkEntityManager, IcebergTablePropertyManager}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DataDog, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3StoragePath
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{GenericBackfillStreamingMergeDataProvider, GenericBackfillStreamingOverwriteDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{GenericGraphBuilderFactory, GenericStreamingGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.{BackfillApplyBatchProcessor, BackfillOverwriteWatermarkProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, WatermarkProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import zio.*
import zio.logging.backend.SLF4J
import zio.metrics.jvm.DefaultJvmMetrics

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val blobSourceLayer: ZLayer[S3BlobStorageReader & PluginStreamContext, Throwable, BlobListingParquetSource[S3StoragePath]] = BlobListingParquetSource.getLayer(context =>
    context.asInstanceOf[ParquetPluginStreamContext].source.configuration
  )
  val s3ReaderLayer: ZLayer[PluginStreamContext, Nothing, S3BlobStorageReader] = DefaultS3Reader.getLayer(context => context.asInstanceOf[ParquetPluginStreamContext].source.configuration)


  private def getExitCode(exception: Throwable): zio.ExitCode =
    exception match
      case _: StreamFailException => zio.ExitCode(2)
      case _                      => zio.ExitCode(1)

  private lazy val streamRunner = appLayer.provide(
    GenericStreamRunnerService.layer,
    GenericGraphBuilderFactory.composedLayer,
    DisposeBatchProcessor.layer,
    FieldFilteringTransformer.layer,
    MergeBatchProcessor.layer,
    StagingProcessor.layer,
    FieldsFilteringService.layer,
    PosixStreamLifetimeService.layer,
    BlobSourceStreamingDataProvider.layer,
    UpsertBlobBackfillOverwriteBatchFactory.layer,
    s3ReaderLayer,
    blobSourceLayer,
    BlobSourceDataProvider.layer,
    ParquetPluginStreamContext.layer,
    IcebergS3CatalogWriter.layer,
    IcebergEntityManager.sinkLayer,
    IcebergEntityManager.stagingLayer,
    IcebergTablePropertyManager.stagingLayer,
    IcebergTablePropertyManager.sinkLayer,
    JdbcMergeServiceClient.layer,
    UpsertBlobHookManager.layer,
    BackfillApplyBatchProcessor.layer,
    GenericBackfillStreamingOverwriteDataProvider.layer,
    GenericBackfillStreamingMergeDataProvider.layer,
    GenericStreamingGraphBuilder.backfillSubStreamLayer,
    DeclaredMetrics.layer,
    ArcaneDimensionsProvider.layer,
    DataDog.UdsPublisher.layer,
    WatermarkProcessor.layer,
    BackfillOverwriteWatermarkProcessor.layer,
    DefaultStreamBootstrapper.layer,
    ThroughputShaperBuilder.layer,
    DefaultJvmMetrics.liveV2.unit
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
