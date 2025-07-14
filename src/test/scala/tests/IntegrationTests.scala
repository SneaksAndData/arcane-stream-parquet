package com.sneaksanddata.arcane.stream_parquet
package tests

import zio.Scope
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault}

object IntegrationTests extends ZIOSpecDefault:
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IntegrationTests")(
    test("runs backfill") {
      for
        
      yield  
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
