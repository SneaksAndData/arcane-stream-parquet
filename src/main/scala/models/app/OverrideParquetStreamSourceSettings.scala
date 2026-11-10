package com.sneaksanddata.arcane.stream_parquet
package models.app

import com.sneaksanddata.arcane.framework.models.settings.sources.blob.DefaultParquetBlobSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultOverrideDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultOverrideSourceBufferingSettings,
  OverrideStreamSourceSettings
}
import upickle.ReadWriter

/** A partial override of `ParquetStreamSourceSettings` provided by the Arcane operator via the environment variable.
  */
case class OverrideParquetStreamSourceSettings(
    override val buffering: Option[DefaultOverrideSourceBufferingSettings] = None,
    override val configuration: Option[DefaultParquetBlobSourceSettings] = None,
    override val modifications: Option[DefaultOverrideDataRowModificationSettings] = None
) extends OverrideStreamSourceSettings derives ReadWriter:
  override type SourceSettingsOverrideType = DefaultParquetBlobSourceSettings
