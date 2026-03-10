package com.sneaksanddata.arcane.stream_parquet
package models.app

import com.sneaksanddata.arcane.framework.models.settings.{
  DefaultFieldSelectionRuleSettings,
  FieldSelectionRuleSettings
}
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultSourceBufferingSettings,
  SourceBufferingSettings,
  StreamSourceSettings
}
import com.sneaksanddata.arcane.framework.models.settings.sources.blob.{
  DefaultParquetBlobSourceSettings,
  ParquetBlobSourceSettings
}
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3ClientSettings
import upickle.ReadWriter

case class ParquetStreamSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val fieldSelectionRule: DefaultFieldSelectionRuleSettings,
    override val configuration: DefaultParquetBlobSourceSettings
) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultParquetBlobSourceSettings
