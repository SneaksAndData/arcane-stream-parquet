package com.sneaksanddata.arcane.stream_parquet

import models.app.ParquetPluginStreamContext

import com.sneaksanddata.arcane.framework.extensions.ZExtensions.*
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.parquets3.Services
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.blobsource.DefaultS3Service
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetStreamingSource
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3StoragePath
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageService
import zio.*
import zio.logging.backend.SLF4J

object main extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val s3ReaderLayer: ZLayer[PluginStreamContext, Nothing, S3BlobStorageService] =
    DefaultS3Service.getLayer(context => context.asInstanceOf[ParquetPluginStreamContext].source.configuration)

  val blobSourceLayer: ZLayer[PluginStreamContext & S3BlobStorageService, Throwable, BlobListingParquetStreamingSource[
    S3StoragePath
  ]] =
    s3ReaderLayer >>> DefaultNameGenerator.layer >>> BlobListingParquetStreamingSource.getS3Layer(context =>
      context.asInstanceOf[ParquetPluginStreamContext].source.configuration
    )
  private lazy val streamRunner = appLayer.provide(
    s3ReaderLayer,
    // streaming
    blobSourceLayer,
    Services.sourceLayer,
    LayerAssemblies.frameworkPipelineServicesLayer,
    LayerAssemblies.frameworkStagingServicesLayer,
    ParquetPluginStreamContext.layer,
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] = streamRunner.handleAppFailure(exit)
