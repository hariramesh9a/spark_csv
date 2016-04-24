package com.examples.employee

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{FunSpec, BeforeAndAfterAll}

class EmployeeDbTest extends FunSpec with BeforeAndAfterAll {
  
  private var sparkContext: SparkContext = _
  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext("local[8]", "EmployeeDB unit tests")
    sqlContext = new HiveContext(sparkContext)
    sqlContext.setConf("spark.sql.shuffle.partitions", "2")
    EmployeeDb.createTables(sqlContext, "src/test/resources/employee_db/")
  }

  override def afterAll(): Unit = {
    try {
      sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }
  
  describe("reading max salary takes date into account"){
    it("reads current data for current date") {
      val latestMaxSalary = EmployeeDb.readMaxSalary(sqlContext, "'2016-03-22'")(0)
      assert(Row("10001", "Facello", "Georgi", 88958.0d, "Manager") == latestMaxSalary)
    }
    
    it("reads historical data for historical date") {
      val olderMaxSalary = EmployeeDb.readMaxSalary(sqlContext, "'1982-01-02'")(0)
      assert(Row("10003", "Bamford", "Parto", 40006.0d, "Janitor") == olderMaxSalary)
    }
  }
  
  describe("reading count of employees in department takes date into account"){
    it("reads current data for current date") {
      val countOfEmployeesByDepartmentCurrent = EmployeeDb.readCountOfEmployeesInDepartments(sqlContext, "'2016-03-22'")
      assert(Set[Row](Row("Finance", 1), Row("Marketing", 1)) == countOfEmployeesByDepartmentCurrent.toSet)
    }
    
    it("reads historical data for historical date") {
      val countOfEmployeesByDepartmentHistorical = EmployeeDb.readCountOfEmployeesInDepartments(sqlContext, "'1985-01-01'")
      assert(Set[Row](Row("Marketing", 2)) == countOfEmployeesByDepartmentHistorical.toSet)
    }
  }
  
  describe("reading manager of each department takes date into account"){
    it("reads current data for current date") {
      val managersCurrent = EmployeeDb.readManagersOfDepartment(sqlContext, "'2016-03-22'")
      assert(Set[Row](Row("Marketing", "Koblick", "Chirstian", "10004"), 
                      Row("Finance", "Simmel", "Bezalel", "10002")) == 
             managersCurrent.toSet)
    }
    
    it("reads historical data for historical date") {
      val managersHistorical = EmployeeDb.readManagersOfDepartment(sqlContext, "'1985-01-01'")
      assert(Set[Row](Row("Marketing", "Facello", "Georgi", "10001"), 
                      Row("Finance", "Bamford", "Parto", "10003")) == 
             managersHistorical.toSet)
    }
  }
  
  describe("reading counts of employees by title takes date into account"){
    it("reads current data for current date") {
      val employeesWithTitleCurrent = EmployeeDb.readCountOfEmployeesByTitle(sqlContext, "'2016-03-22'")
      assert(Set[Row](Row("Manager", 4)) == employeesWithTitleCurrent.toSet)
    }
    
    it("reads historical data for historical date") {
      val employeesWithTitleHistorical = EmployeeDb.readCountOfEmployeesByTitle(sqlContext, "'1985-01-01'")
      assert(Set[Row](Row("Turtle Keeper", 1), Row("Manager", 2)) == employeesWithTitleHistorical.toSet)
    }
  }
  
}