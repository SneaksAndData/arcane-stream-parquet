package com.sneaksanddata.arcane.stream_parquet
package tests

import models.UpsertBlobStreamContext
import models.app.StreamSpec
import tests.Common.StreamContextLayer

import zio.test.TestAspect.timeout
import zio.test.*
import zio.{Scope, ULayer, ZIO, ZLayer}

import java.time.Duration

object IntegrationTests extends ZIOSpecDefault:
  val targetTableName = "iceberg.test.stream_run"

  private val streamContextStr =
    s"""
       |
       |{
       |  "backfillJobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-parquet-large-job"
       |  },
       |  "groupingIntervalSeconds": 1,
       |  "jobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-parquet-standard-job"
       |  },
       |  "lookBackInterval": 300,
       |  "tableProperties": {
       |    "partitionExpressions": [],
       |    "format": "PARQUET",
       |    "sortedBy": [],
       |    "parquetBloomFilterColumns": []
       |  },
       |  "rowsPerGroup": 1000,
       |  "sinkSettings": {
       |    "optimizeSettings": {
       |      "batchThreshold": 60,
       |      "fileSizeThreshold": "512MB"
       |    },
       |    "orphanFilesExpirationSettings": {
       |      "batchThreshold": 60,
       |      "retentionThreshold": "6h"
       |    },
       |    "snapshotExpirationSettings": {
       |      "batchThreshold": 60,
       |      "retentionThreshold": "6h"
       |    },
       |    "analyzeSettings": {
       |      "batchThreshold": 60,
       |      "includedColumns": []
       |    },
       |    "targetTableName": "$targetTableName"
       |  },
       |  "sourceSettings": {
       |    "changeCaptureIntervalSeconds": 5,
       |    "baseLocation": "s3a://s3-blob-reader",
       |    "tempPath": "/tmp",
       |    "primaryKeys": ["col0"],
       |    "useNameMapping": false,
       |    "s3": {
       |      "usePathStyle": true,
       |      "region": "us-east-1",
       |      "endpoint": "http://localhost:9000",
       |      "maxResultsPerPage": 150,
       |      "retryMaxAttempts": 5,
       |      "retryBaseDelay": 0.1,
       |      "retryMaxDelay": 1
       |    }
       |  },
       |  "stagingDataSettings": {
       |    "catalog": {
       |      "catalogName": "iceberg",
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "schemaName": "test",
       |      "warehouse": "demo"
       |    },
       |    "tableNamePrefix": "staging_parquet_test",
       |    "maxRowsPerFile": 10000
       |  },
       |  "fieldSelectionRule": {
       |    "ruleType": "all",
       |    "fields": []
       |  },
       |  "backfillBehavior": "overwrite",
       |  "backfillStartDate": "1735731264"
       |}""".stripMargin

  private val parsedSpec = StreamSpec.fromString(streamContextStr)

  private val streamingStreamContext = new UpsertBlobStreamContext(parsedSpec):
    override val IsBackfilling: Boolean = false

  private val backfillStreamContext = new UpsertBlobStreamContext(parsedSpec):
    override val IsBackfilling: Boolean = true

  private val streamingStreamContextLayer: ULayer[UpsertBlobStreamContext] =
    ZLayer.succeed[UpsertBlobStreamContext](streamingStreamContext)

  private val backfillStreamContextLayer = ZLayer.succeed[UpsertBlobStreamContext](backfillStreamContext)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("runs backfill") {
      for
        _              <- ZIO.attempt(Fixtures.clearTarget(targetTableName))
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, backfillStreamContextLayer).fork
        _ <- Common.waitForData(
          backfillStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder,
          100 // col0 only have 100 unique values, thus we expect 100 rows total
        )
        _ <- backfillRunner.await.timeout(Duration.ofSeconds(10))
      yield assertTrue(true)
    },
    test("runs stream correctly") {
      for
        streamRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
        rows <- Common.getData(
          streamingStreamContext.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
        _ <- streamRunner.await.timeout(Duration.ofSeconds(10))
      yield assertTrue(rows.size == 100) // no new rows added after stream has started
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
