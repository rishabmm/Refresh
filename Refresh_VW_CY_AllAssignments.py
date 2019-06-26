# Databricks notebook source
# DBTITLE 1,VERSION HISTORY
# MAGIC %md
# MAGIC 
# MAGIC ###### Project Name: CRM
# MAGIC 
# MAGIC ###### Purpose: Notebook to Refresh VW_CY_AllAssignments
# MAGIC 
# MAGIC ###### Parameter Info:
# MAGIC 
# MAGIC ###### Revision History:
# MAGIC 
# MAGIC | Date     |     Author    |  Description  |  Execution Time  |
# MAGIC |----------|:-------------:|--------------:|
# MAGIC |APR 5, 2019|v-neshah|Created NoteBook to  Refresh VW_CY_AllAssignments For CRM||

# COMMAND ----------

# MAGIC %run /Tools/NotebookExecution

# COMMAND ----------

# MAGIC %scala
# MAGIC val path=dbutils.notebook.getContext().notebookPath
# MAGIC val b=path.get.toString.split("/")
# MAGIC val NotebookPath=b(b.size-1)

# COMMAND ----------

# MAGIC 
# MAGIC %scala
# MAGIC val Result=GetNotebookStatus(NotebookPath,StreamName = "CRM")
# MAGIC if(Result.contains(0)){
# MAGIC dbutils.notebook.exit("0")
# MAGIC }
# MAGIC else if(Result.contains(-1)){
# MAGIC System.exit(-1)
# MAGIC }
# MAGIC else if(Result.contains(2)){
# MAGIC dbutils.notebook.exit("2")
# MAGIC }

# COMMAND ----------

# MAGIC %run /Tools/ApplicationInsightsSetup

# COMMAND ----------

# Creating widgets for leveraging parameters, and printing the parameters
dbutils.widgets.text("DataFactoryName", "","")
dbutils.widgets.text("PipelineName", "","")
dbutils.widgets.text("PipelineRunId", "","")

DataFactoryName = dbutils.widgets.get("DataFactoryName")
PipelineName = dbutils.widgets.get("PipelineName")
PipelineRunId = dbutils.widgets.get("PipelineRunId")

telemetryClient = NewTelemetryClient("CRM", DataFactoryName, PipelineName, PipelineRunId)

# COMMAND ----------

#start logging cell
start("Running Stored Procedure for: VW_CY_AllAssignments")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ocpmart_pipeline_ipcosell

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table ocpstaging_smdp_ipcosell.vw_CY_AllAssignments;

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/ocpmart_pipeline_ipcosell.db/cy_allassignments",True);


# COMMAND ----------

start("Creating table:[CY_AllAssignments]")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS CY_AllAssignments;
# MAGIC     CREATE TABLE CY_AllAssignments(
# MAGIC 	      AccountSellerAssignmentGUID STRING 
# MAGIC          ,AreaName					  VARCHAR(100) 
# MAGIC          ,AssignmentEndDate			  TIMESTAMP 
# MAGIC          ,AssignmentStartDate		  TIMESTAMP 
# MAGIC          ,AssignmentStatus			  VARCHAR(30) 
# MAGIC          ,ContractDerivedID			  BIGINT 
# MAGIC          ,ContractNumber			  VARCHAR(40) 
# MAGIC          ,ContractScheduleId		  INT 
# MAGIC          ,CreatedByAlias	          VARCHAR(70)  
# MAGIC          ,CreatedDate	              TIMESTAMP 
# MAGIC          ,CRMAccountID	              VARCHAR(30) 
# MAGIC          ,CustomerSalesAccountID	  INT  
# MAGIC          ,DataSource	              VARCHAR(100) 
# MAGIC          ,EmailAlias	              VARCHAR(60) 
# MAGIC          ,GSSGUID	                  STRING
# MAGIC          ,IsTBH	                      INT 
# MAGIC          ,IsUserActive	              INT  
# MAGIC          ,LastModifiedByAlias	      VARCHAR(70)  
# MAGIC          ,LastModifiedDate	          TIMESTAMP
# MAGIC 	     ,MSSalesTPID                 INT 
# MAGIC          ,PersonnelNumber	          INT 
# MAGIC          ,PlatformCreatedDate	      TIMESTAMP 
# MAGIC          ,PlatformModifiedDate 	      TIMESTAMP 
# MAGIC          ,PositionNumber			  INT 
# MAGIC          ,Qualifier1				  VARCHAR(50)  
# MAGIC          ,Qualifier2				  VARCHAR(50) 
# MAGIC          ,ReportingManagerAlias	 	  VARCHAR(60) 
# MAGIC          ,Role					 	  VARCHAR(510)  
# MAGIC          ,RoleType				 	  VARCHAR(60)  
# MAGIC          ,StandardTitle			 	  VARCHAR(80)  
# MAGIC          ,SubsidiaryName		      VARCHAR(100)   
# MAGIC          ,AccountName				  STRING
# MAGIC       )

# COMMAND ----------

start("Inserting into table:[CY_AllAssignments]")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO CY_AllAssignments
# MAGIC SELECT     CAA.AccountSellerAssignmentGUID
# MAGIC 		  ,CAA.AreaName
# MAGIC 		  ,CAA.AssignmentEndDate AS AssignmentEndDate
# MAGIC 		  ,CAA.AssignmentStartDate AS AssignmentStartDate
# MAGIC 		  ,CAA.AssignmentStatus
# MAGIC 		  ,CAA.ContractDerivedID
# MAGIC 		  ,CAA.ContractNumber
# MAGIC 		  ,CAA.ContractScheduleId
# MAGIC 		  ,CAA.CreatedByAlias
# MAGIC           ,CAA.CreatedDate AS CreatedDate
# MAGIC 		  ,CAA.CRMAccountID
# MAGIC 		  ,CAA.CustomerSalesAccountID
# MAGIC 		  ,CAA.DataSource
# MAGIC 		  ,CAA.EmailAlias
# MAGIC 		  ,CAA.GSSGUID
# MAGIC 		  ,CASE WHEN lower(CAA.IsTBH) ='true' THEN 1 ELSE 0 END AS IsTBH
# MAGIC 		  ,CASE WHEN lower(CAA.IsUserActive) ='true' THEN 1 ELSE 0 END AS IsUserActive
# MAGIC 		  ,CAA.LastModifiedByAlias
# MAGIC 		  ,CAA.LastModifiedDate AS LastModifiedDate
# MAGIC 		  ,CAA.MSSalesTPID AS MSSalesTPID
# MAGIC           ,CAA.PersonnelNumber
# MAGIC 		  ,CAA.PlatformCreatedDate AS PlatformCreatedDate
# MAGIC 		  ,CAA.PlatformModifiedDate AS PlatformModifiedDate
# MAGIC 		  ,CAA.PositionNumber
# MAGIC 		  ,CAA.Qualifier1
# MAGIC 		  ,CAA.Qualifier2
# MAGIC 		  ,CAA.ReportingManagerAlias
# MAGIC 		  ,CAA.Role
# MAGIC 		  ,CAA.RoleType
# MAGIC 		  ,CAA.StandardTitle
# MAGIC 		  ,CAA.SubsidiaryName
# MAGIC 		  ,CAA.AccountName
# MAGIC     FROM ocpstaging_smdp_ipcosell.vw_CY_AllAssignments CAA
# MAGIC 	WHERE CAA.AssignmentStatus = 'ACTIVE'
# MAGIC  	AND (CAA.StandardTitle LIKE '%Channel Manager%' OR CAA.StandardTitle IN ('Account Executive'
# MAGIC 			,'Principal Solution Specialist'
# MAGIC 			,'Solution Specialist'
# MAGIC 			,'Cloud Specialist'
# MAGIC 			,'Opportunity Manager'
# MAGIC 			,'Inside Account Executive'
# MAGIC 			,'Inside Solution Specialist'
# MAGIC 			))

# COMMAND ----------

end("Successfully created table: [CY_AllAssignments]")

# COMMAND ----------

#end of notebook
end()

# COMMAND ----------

# MAGIC %scala
# MAGIC SetNotebookStatus(NotebookPath)