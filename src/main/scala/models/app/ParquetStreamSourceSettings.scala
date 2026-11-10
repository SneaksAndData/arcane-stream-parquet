package com.sneaksanddata.arcane.stream_parquet
package models.app

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import com.sneaksanddata.arcane.framework.models.settings.sources.blob.DefaultParquetBlobSourceSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import upickle.ReadWriter

case class ParquetStreamSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val modifications: DefaultDataRowModificationSettings,
    override val configuration: DefaultParquetBlobSourceSettings
) extends StreamSourceSettings,
      Mergeable derives ReadWriter:
  override type SourceSettingsType = DefaultParquetBlobSourceSettings

  override type MergeableFrom = OverrideParquetStreamSourceSettings
  override type MergeResult   = ParquetStreamSourceSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    ParquetStreamSourceSettings(
      buffering = this.buffering.merge(overrides.flatMap(_.buffering)),
      modifications = this.modifications.merge(overrides.flatMap(_.modifications)),
      configuration
    )
