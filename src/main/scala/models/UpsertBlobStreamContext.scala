package com.sneaksanddata.arcane.stream_parquet
package models

import models.app.StreamSpec

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings
import com.sneaksanddata.arcane.framework.models.settings.blob.ParquetBlobSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.{
  AnalyzeSettings,
  BackfillBehavior,
  BackfillSettings,
  BufferingStrategy,
  FieldSelectionRule,
  FieldSelectionRuleSettings,
  GroupingSettings,
  IcebergCatalogSettings,
  JdbcMergeServiceClientSettings,
  OptimizeSettings,
  OrphanFilesExpirationSettings,
  SnapshotExpirationSettings,
  SourceBufferingSettings,
  StagingDataSettings,
  TableFormat,
  TableMaintenanceSettings,
  TablePropertiesSettings,
  TargetTableSettings,
  VersionedDataGraphBuilderSettings
}
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergCatalogCredential
import com.sneaksanddata.arcane.framework.services.iceberg.base.S3CatalogFileIO
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3ClientSettings
import com.sneaksanddata.arcane.framework.services.storage.services.s3.S3BlobStorageReader
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, EnvironmentVariableCredentialsProvider}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.{ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.util.Try

/** The context for the UpsertBlob Parquet stream.
  *
  * @param spec
  *   The stream specification
  */
case class UpsertBlobStreamContext(spec: StreamSpec)
    extends StreamContext
    with GroupingSettings
    with IcebergCatalogSettings
    with JdbcMergeServiceClientSettings
    with VersionedDataGraphBuilderSettings
    with TargetTableSettings
    with TablePropertiesSettings
    with FieldSelectionRuleSettings
    with BackfillSettings
    with StagingDataSettings
    with ParquetBlobSourceSettings
    with SourceBufferingSettings:

  override val rowsPerGroup: Int =
    System.getenv().getOrDefault("STREAMCONTEXT__ROWS_PER_GROUP", spec.rowsPerGroup.toString).toInt

  override val lookBackInterval: Duration      = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.sourceSettings.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration      = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String               = spec.stagingDataSettings.catalog.namespace
  override val warehouse: String               = spec.stagingDataSettings.catalog.warehouse
  override val catalogUri: String              = spec.stagingDataSettings.catalog.catalogUri
  override val stagingLocation: Option[String] = spec.stagingDataSettings.dataLocation

  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => Map()
    case None    => IcebergCatalogCredential.oAuth2Properties

  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.sinkSettings.targetTableName

  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings:
    override val targetOptimizeSettings: Option[OptimizeSettings] = Some(new OptimizeSettings {
      override val batchThreshold: Int       = spec.sinkSettings.optimizeSettings.batchThreshold
      override val fileSizeThreshold: String = spec.sinkSettings.optimizeSettings.fileSizeThreshold
    })

    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(
      new SnapshotExpirationSettings {
        override val batchThreshold: Int        = spec.sinkSettings.snapshotExpirationSettings.batchThreshold
        override val retentionThreshold: String = spec.sinkSettings.snapshotExpirationSettings.retentionThreshold
      }
    )

    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(
      new OrphanFilesExpirationSettings {
        override val batchThreshold: Int        = spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold
        override val retentionThreshold: String = spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold

      }
    )

    override val targetAnalyzeSettings: Option[AnalyzeSettings] = Some(
      new AnalyzeSettings {
        override val batchThreshold: Int          = spec.sinkSettings.analyzeSettings.batchThreshold
        override val includedColumns: Seq[String] = spec.sinkSettings.analyzeSettings.includedColumns
      }
    )

  val s3ClientSettings: S3ClientSettings = S3ClientSettings(
    pathStyleAccess = spec.sourceSettings.s3.usePathStyle,
    region = Some(spec.sourceSettings.s3.region),
    endpoint = Some(spec.sourceSettings.s3.endpoint),
    maxResultsPerPage = spec.sourceSettings.s3.maxResultsPerPage,
    retryBaseDelay = Duration.ofMillis((spec.sourceSettings.s3.retryBaseDelay * 1000).toLong),
    retryMaxDelay = Duration.ofMillis((spec.sourceSettings.s3.retryMaxDelay * 1000).toLong),
    retryMaxAttempts = spec.sourceSettings.s3.retryMaxAttempts
  )

  override val stagingTablePrefix: String = spec.stagingDataSettings.tableNamePrefix
  val stagingCatalogName: String          = spec.stagingDataSettings.catalog.catalogName
  val stagingSchemaName: String           = spec.stagingDataSettings.catalog.schemaName

  val partitionExpressions: Array[String] = spec.tableProperties.partitionExpressions

  val format: TableFormat                      = TableFormat.valueOf(spec.tableProperties.format)
  val sortedBy: Array[String]                  = spec.tableProperties.sortedBy
  val parquetBloomFilterColumns: Array[String] = spec.tableProperties.parquetBloomFilterColumns

  val backfillTableFullName: String =
    s"$stagingCatalogName.$stagingSchemaName.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}"
      .replace('-', '_')

  override val rule: FieldSelectionRule = spec.fieldSelectionRule.ruleType match
    case "include" => FieldSelectionRule.IncludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case "exclude" => FieldSelectionRule.ExcludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case _         => FieldSelectionRule.AllFields

  override val essentialFields: Set[String] = Set("createdon", "arcane_merge_key")

  override val backfillBehavior: BackfillBehavior = spec.backfillBehavior match
    case "merge"     => BackfillBehavior.Merge
    case "overwrite" => BackfillBehavior.Overwrite
    case _           => throw new IllegalArgumentException(s"Unknown backfill behavior: ${spec.backfillBehavior}")
  override val backfillStartDate: Option[OffsetDateTime] = Some(
    Instant.ofEpochSecond(spec.backfillStartDate.toLong).atOffset(ZoneOffset.UTC)
  )

  override val changeCaptureIntervalSeconds: Int = spec.sourceSettings.changeCaptureIntervalSeconds

  override val maxRowsPerFile: Option[Int] = Some(spec.stagingDataSettings.maxRowsPerFile)

  override val bufferingEnabled: Boolean            = false
  override val bufferingStrategy: BufferingStrategy = BufferingStrategy.Buffering(0)

  override val isUnifiedSchema: Boolean  = true
  override val isServerSide: Boolean     = false
  override val sourcePath: String        = spec.sourceSettings.baseLocation
  override val tempStoragePath: String   = spec.sourceSettings.tempPath
  override val primaryKeys: List[String] = spec.sourceSettings.primaryKeys

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")
  val metricsPublisherInterval: Duration = Duration.ofMillis(
    sys.env.getOrElse("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS", "100").toInt
  )

given Conversion[UpsertBlobStreamContext, DatagramSocketConfig] with
  def apply(context: UpsertBlobStreamContext): DatagramSocketConfig =
    DatagramSocketConfig(context.datadogSocketPath)

given Conversion[UpsertBlobStreamContext, MetricsConfig] with
  def apply(context: UpsertBlobStreamContext): MetricsConfig =
    MetricsConfig(context.metricsPublisherInterval)

object UpsertBlobStreamContext:

  type Environment = StreamContext & GroupingSettings & VersionedDataGraphBuilderSettings & IcebergCatalogSettings &
    JdbcMergeServiceClientSettings & TargetTableSettings & UpsertBlobStreamContext & TablePropertiesSettings &
    FieldSelectionRuleSettings & BackfillSettings & StagingDataSettings & ParquetBlobSourceSettings &
    SourceBufferingSettings & MetricsConfig & DatagramSocketConfig & DatadogPublisherConfig

  /** The ZLayer that creates the VersionedDataGraphBuilder.
    */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
    .fromEnvironment("STREAMCONTEXT__SPEC")
    .map(combineSettingsLayer)
    .getOrElse(ZLayer.fail(new Exception("The stream context failed to initialize.")))

  private def combineSettingsLayer(spec: StreamSpec): ZLayer[Any, Throwable, Environment] =
    val context = UpsertBlobStreamContext(spec)

    ZLayer.succeed(context)
      ++ ZLayer.succeed[DatagramSocketConfig](context)
      ++ ZLayer.succeed[MetricsConfig](context)
      ++ ZLayer.succeed(DatadogPublisherConfig())

object S3Reader:
  val layer: ZLayer[UpsertBlobStreamContext, Nothing, S3BlobStorageReader] = ZLayer {
    for
      context  <- ZIO.service[UpsertBlobStreamContext]
      settings <- ZIO.succeed(Some(context.s3ClientSettings))
    yield S3BlobStorageReader(
      credentialsProvider = DefaultCredentialsProvider.create(),
      settings = settings
    )
  }
