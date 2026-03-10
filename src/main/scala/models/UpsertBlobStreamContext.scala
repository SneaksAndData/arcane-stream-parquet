///** The context for the UpsertBlob Parquet stream.
//  *
//  * @param spec
//  *   The stream specification
//  */
//case class UpsertBlobStreamContext(spec: ParquetStreamSpec)
//    extends StreamContext
//    with GroupingSettings
//    with IcebergStagingSettings
//    with JdbcMergeServiceClientSettings
//    with VersionedDataGraphBuilderSettings
//    with SinkSettings
//    with TablePropertiesSettings
//    with FieldSelectionRuleSettings
//    with BackfillSettings
//    with StagingDataSettings
//    with ParquetBlobSourceSettings
//    with SourceBufferingSettings:
//
//  override def customTags: Map[String, String] = spec.observabilitySettings.metricTags
//
//  override val rowsPerGroup: Int =
//    System.getenv().getOrDefault("STREAMCONTEXT__ROWS_PER_GROUP", spec.rowsPerGroup.toString).toInt
//
//  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
//    case Some(_) => S3CatalogFileIO.properties
//    case None    => S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties
//
//  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
//
//  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")
//
//
//  val backfillTableFullName: String =
//    s"$stagingCatalogName.$stagingSchemaName.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}"
//      .replace('-', '_')
//
//  override val essentialFields: Set[String] = Set("createdon", "arcane_merge_key")
//
//
//  override val bufferingEnabled: Boolean            = false
//  override val bufferingStrategy: BufferingStrategy = BufferingStrategy.Buffering(0)
//
//  override val isUnifiedSchema: Boolean  = true
//  override val isServerSide: Boolean     = false
