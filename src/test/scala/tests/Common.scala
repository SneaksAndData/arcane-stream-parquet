package com.sneaksanddata.arcane.stream_parquet
package tests

import main.appLayer
import models.{S3Reader, UpsertBlobStreamContext}

import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.blobsource.providers.BlobSourceStreamingDataProvider
import com.sneaksanddata.arcane.framework.services.blobsource.readers.listing.BlobListingParquetSource
import com.sneaksanddata.arcane.framework.services.blobsource.{
  UpsertBlobBackfillOverwriteBatchFactory,
  UpsertBlobHookManager
}
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{
  GenericBackfillStreamingMergeDataProvider,
  GenericBackfillStreamingOverwriteDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{
  GenericGraphBuilderFactory,
  GenericStreamingGraphBuilder
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import zio.{ZIO, ZLayer}

/** Common utilities for tests.
 */
object Common:

  type StreamLifeTimeServiceLayer = ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken]
  type StreamContextLayer         = ZLayer[Any, Nothing, UpsertBlobStreamContext.Environment]

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
    )
