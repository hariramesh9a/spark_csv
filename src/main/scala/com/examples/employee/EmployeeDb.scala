package com.examples.employee

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object EmployeeDb {
  
  def createTables(sqlContext: SQLContext, employeesDbDir: String): SQLContext = {
    
    val read = sqlContext.read.format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("dateFormat", "yyyy-MM-dd")

          
    read.schema(StructType(Array(StructField("department_code", StringType, false),
                                 StructField("department_name", StringType, false))))
        .load(employeesDbDir + "departments.csv")
        .registerTempTable("department")
    
    read.schema(StructType(Array(StructField("employee_id", StringType, false),
                                 StructField("department_code", StringType, false),
                                 StructField("start_date", DateType, false),
                                 StructField("end_date", DateType, false))))
        .load(employeesDbDir + "dept_emp_rlshp.csv")
        .registerTempTable("dept_emp_rlshp")
    
    read.schema(StructType(Array(StructField("department_code", StringType, false),
                                 StructField("employee_id", StringType, false),
                                 StructField("start_date", DateType, false),
                                 StructField("end_date", DateType, false))))
        .load(employeesDbDir + "dept_manager.csv")
        .registerTempTable("dept_manager")
    
    read.schema(StructType(Array(StructField("employee_id", StringType, false),
                                 StructField("dob", DateType, false),
                                 StructField("first_name", StringType, false),
                                 StructField("last_name", StringType, false),
                                 StructField("gender", StringType, false),
                                 StructField("hire_date", DateType, false))))
        .load(employeesDbDir + "employees.csv")
        .registerTempTable("employee")
    
    read.schema(StructType(Array(StructField("employee_id", StringType, false),
                                 StructField("salary_dollars", DoubleType, false),
                                 StructField("start_date", DateType, false),
                                 StructField("end_date", DateType, false))))
        .load(employeesDbDir + "salaries.csv")
        .registerTempTable("salary")
    
    read.schema(StructType(Array(StructField("employee_id", StringType, false),
                                 StructField("title", StringType, false),
                                 StructField("start_date", DateType, false),
                                 StructField("end_date", DateType, false))))
        .load(employeesDbDir + "titles.csv")
        .registerTempTable("title")
    
    return sqlContext
  }
  
  def readMaxSalary(sqlContext: SQLContext, today: String): Array[Row] = {
    sqlContext.sql("""select emp.employee_id, emp.last_name, emp.first_name, salary.salary_dollars, title.title 
                        from salary, employee emp, title
                          where salary.employee_id = emp.employee_id
                            and title.employee_id = emp.employee_id
                            and salary.start_date <= """ + today + """
                            and salary.end_date > """ + today + """
                            and title.start_date <= """ + today + """
                            and title.end_date > """ + today + """
                            and emp.hire_date <= """ + today + """
                        order by salary_dollars desc""").take(1)
  }
  
  def readCountOfEmployeesInDepartments(sqlContext: SQLContext, today: String): Array[Row] = {
    sqlContext.sql("""select dept.department_name, count(*) 
                        from department dept, dept_emp_rlshp emp_rlshp 
                        where dept.department_code = emp_rlshp.department_code
                          and emp_rlshp.start_date <= """ + today + """
                          and emp_rlshp.end_date > """ + today + """
                        group by dept.department_name""").collect()
  }
  
  def readManagersOfDepartment(sqlContext: SQLContext, today: String): Array[Row] = {
    sqlContext.sql("""select dpt.department_name, emp.last_name, emp.first_name, emp.employee_id 
                        from employee emp, dept_manager mgr, department dpt
                        where mgr.employee_id = emp.employee_id
                          and dpt.department_code = mgr.department_code
                          and mgr.start_date <= """ + today + """
                          and mgr.end_date > """ + today).collect()
  }
  
  def readCountOfEmployeesByTitle(sqlContext: SQLContext, today: String): Array[Row] = {
    sqlContext.sql("""select title.title, count(*) 
                        from title
                        where title.start_date <= """ + today + """
                          and title.end_date > """ + today + """
                        group by title.title""").collect()
  }
  
  def createReport(sqlContext: SQLContext, today: String): String = {
    
    def readrows(name: String, f: () => Array[Row]): String = {
      val nl = System.getProperty("line.separator")
      val rowsBroken = new StringBuilder()
      val start = System.currentTimeMillis()
      val rows = f()
      rows.foreach { row => rowsBroken.append(row).append(nl) }
      name + rows.size + " rows in " + (System.currentTimeMillis() - start) + "ms." + nl + rowsBroken + nl
    }
    
    readrows("max salary: ", () => readMaxSalary(sqlContext, today)) +
      readrows("people actively employed in each department: ", () => readCountOfEmployeesInDepartments(sqlContext, today)) +
      readrows("managers by department: ", () => readManagersOfDepartment(sqlContext, today)) +
      readrows("people currently with title: ", () => readCountOfEmployeesByTitle(sqlContext, today))
  }
  
  def main(arg: Array[String]) {    
    val sqlContext = new HiveContext(new SparkContext("local[8]", "Employee DB - Load"))
    
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    
    println(createReport(createTables(sqlContext, arg(0)), "'" + arg(1) + "'"))
    
    sqlContext.sparkContext.stop()
    
  }

}
