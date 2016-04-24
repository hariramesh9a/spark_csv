package com.examples.employee

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class EmployeeDbIntegrationTest extends FlatSpec with Matchers {
  
  "Running EmployeeDbLoad" should "not fail with an exception" in { // TODO: Integration! Use only to guide development
    val useTestData = false
    EmployeeDb.main(Array[String]("src/" + (if (useTestData) "test" else "main") + "/resources/employee_db/", "2016-03-22"));
  }
  
}