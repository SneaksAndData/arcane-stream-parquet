package com.sneaksanddata.arcane.stream_parquet
package tests

import models.app.ParquetPluginStreamContext
import tests.Common.getLatestVersion

import com.sneaksanddata.arcane.framework.services.blobsource.versioning.BlobSourceWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  clearTarget,
  getWatermark,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}

import java.time.Duration

object IntegrationTests extends ZIOSpecDefault:
  val targetTableName = "iceberg.test.stream_run"

  private val streamContextStr =
    s"""
       |{
       |  "backfillJobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-parquet-large-job"
       |  },
       |  "jobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-parquet-standard-job"
       |  },
       |  "observability": {
       |    "metricTags": {}
       |  },
       |  "staging": {
       |    "table": {
       |      "stagingTablePrefix": "staging_parquet_test",
       |      "maxRowsPerFile": 10000,
       |      "stagingCatalogName": "iceberg",
       |      "stagingSchemaName": "test",
       |      "isUnifiedSchema": false
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo"
       |    }
       |  },
       |  "streamMode": {
       |    "backfill": {
       |      "backfillBehavior": "Overwrite",
       |      "backfillStartDate": "2026-01-01T00:00:00Z"
       |    },
       |    "changeCapture": {
       |      "changeCaptureInterval": "5s",
       |      "changeCaptureJitterVariance": 0.1,
       |      "changeCaptureJitterSeed": 0
       |    }
       |  },
       |  "sink": {
       |    "mergeServiceClient": {
       |      "extraConnectionParameters": {
       |        "client_tags": "test"
       |      },
       |      "queryRetryMode": "Never",
       |      "queryRetryBaseDuration": "100ms",
       |      "queryRetryOnMessageContents": [],
       |      "queryRetryScaleFactor": 0.1,
       |      "queryRetryMaxAttempts": 3
       |    },
       |    "targetTableProperties": {
       |      "format": "PARQUET",
       |      "sortedBy": [],
       |      "parquetBloomFilterColumns": []
       |    },
       |    "targetTableFullName": "$targetTableName",
       |    "maintenanceSettings": {
       |      "targetOptimizeSettings": {
       |        "batchThreshold": 60,
       |        "fileSizeThreshold": "512MB"
       |      },
       |      "targetOrphanFilesExpirationSettings": {
       |        "batchThreshold": 60,
       |        "retentionThreshold": "6h"
       |      },
       |      "targetSnapshotExpirationSettings": {
       |        "batchThreshold": 60,
       |        "retentionThreshold": "6h"
       |      },
       |      "targetAnalyzeSettings": {
       |        "includedColumns": [],
       |        "batchThreshold": 60
       |      }
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo"
       |    }
       |  },
       |  "throughput": {
       |    "shaperImpl": {
       |      "memoryBound": {}
       |    }
       |  },
       |  "source": {
       |    "configuration": {
       |      "sourcePath": "s3a://s3-blob-reader",
       |      "tempStoragePath": "/tmp",
       |      "primaryKeys": ["col0"],
       |      "s3": {
       |        "usePathStyle": true,
       |        "region": "us-east-1",
       |        "endpoint": "http://localhost:9000",
       |        "maxResultsPerPage": 1000,
       |        "retryMaxAttempts": 5,
       |        "retryBaseDelay": 0.1,
       |        "retryMaxDelay": 1
       |      }
       |    },
       |    "buffering": {
       |      "enabled": false,
       |      "strategy": {
       |        "unbounded": {}
       |      }
       |    },
       |    "fieldSelectionRule": {
       |      "all": {}
       |    }
       |  }
       |}""".stripMargin

  private val streamingStreamContext = ParquetPluginStreamContext(streamContextStr)
  private val streamingStreamContextLayer = ZLayer.succeed[ParquetPluginStreamContext](streamingStreamContext)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("runs backfill") {
      for
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _              <- ZIO.attempt(clearTarget(targetTableName))
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(65), streamingStreamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(60))
        rows <- readTarget(
          streamingStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        ) // col0 only have 100 unique values, thus we expect 100 rows total
        watermark <- getWatermark(streamingStreamContext.sink.targetTableFullName.split('.').last)(
          BlobSourceWatermark.rw
        )
        latestVersion <- getLatestVersion
      yield assertTrue(rows.size == 100) implies assertTrue(watermark.version.toLong == latestVersion)
    },
    test("runs stream correctly") {
      for
        streamRunner <- Common.getTestApp(Duration.ofSeconds(15), streamingStreamContextLayer).fork
        _            <- streamRunner.runOrFail(Duration.ofSeconds(10))

        rows <- readTarget(
          streamingStreamContext.sink.targetTableFullName,
          "col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, arcane_merge_key, createdon",
          Common.TargetDecoder
        )
        watermark <- getWatermark(streamingStreamContext.sink.targetTableFullName.split('.').last)(
          BlobSourceWatermark.rw
        )
        latestVersion <- getLatestVersion
      yield assertTrue(rows.size == 100) implies assertTrue(
        watermark.version.toLong == latestVersion
      ) // no new rows added after stream has started
    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
