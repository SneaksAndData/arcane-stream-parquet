package com.sneaksanddata.arcane.stream_parquet
package tests

import java.sql.DriverManager

object Fixtures:

  val trinoConnectionString: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  def clearTarget(targetFullName: String): Any =
    val trinoConnection = DriverManager.getConnection(trinoConnectionString)
    val query           = s"drop table if exists $targetFullName"
    val statement       = trinoConnection.createStatement()
    statement.executeUpdate(query)
