# Databricks notebook source
# DBTITLE 1,VERSION HISTORY
# MAGIC %md
# MAGIC 
# MAGIC ###### Project Name: CRM
# MAGIC 
# MAGIC ###### Purpose: Notebook to Refresh TPAccounts_Dim
# MAGIC 
# MAGIC ###### Parameter Info:
# MAGIC 
# MAGIC ###### Revision History:
# MAGIC 
# MAGIC | Date     |     Author    |  Description  |  Execution Time  |
# MAGIC |----------|:-------------:|--------------:|
# MAGIC |APR 5, 2019|v-neshah|Created NoteBook to  Refresh TPAccounts_Dim For CRM||

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
start("Running Stored Procedure for: TPAccountsDim ")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ocpmart_pipeline_ipcosell

# COMMAND ----------

start("Creating table:[TPAccountsDim]")

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table ocpstaging_smdp_ipcosell.vw_CY_CALCAccountTags;
# MAGIC Refresh table ocpstaging_smdp_ipcosell.DimRevenueAccount;
# MAGIC Refresh table ocpstaging_smdp_ipcosell.DimGeographySubsidiaryHierarchy;
# MAGIC Refresh table ocpstaging_smdp_ipcosell.DimSegmentHierarchy;
# MAGIC Refresh table ocpstaging_smdp_ipcosell.vw_CY_ManagedTopParentAccounts;

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/ocpmart_pipeline_ipcosell.db/tpaccountsdim",True);
dbutils.fs.rm("dbfs:/user/hive/warehouse/ocpmart_pipeline_ipcosell.db/tmphipo_population",True);

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TPAccountsDim;
# MAGIC 		CREATE TABLE TPAccountsDim(
# MAGIC 	    TPAccountKey                 int  ,
# MAGIC 	    TPAccountID                  int  ,
# MAGIC 	    TPAccountName                varchar(60) ,
# MAGIC 	    SalesTerritoryID             int ,
# MAGIC 	    SubsidiarySubDistrictId      int ,
# MAGIC 	    SubsidiarySubDistrictName    varchar(60) ,
# MAGIC 	    MALFlagName                  varchar(3) ,
# MAGIC 	    VerticalID                   tinyint  ,
# MAGIC 	    VerticalName                 varchar(40)  ,
# MAGIC 	    IndustryID                   tinyint  ,
# MAGIC 	    IndustryName                 varchar(40)  ,
# MAGIC 	    SubsidiaryID                 int  ,
# MAGIC 	    SubsidiaryName               varchar(40) ,
# MAGIC 	    SubSegmentID                 int  ,
# MAGIC 	    SubSegmentName               varchar(40)  ,
# MAGIC 	    Status                       varchar(50) ,
# MAGIC 	    SegmentGroup                 varchar(40) ,
# MAGIC 	    `HQ/DS`                      varchar(510) ,
# MAGIC 	    IsHIPOAccount                varchar(3) 
# MAGIC )

# COMMAND ----------

start("Creating table:[TMPHiPo_Population]")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TMPHiPo_Population;
# MAGIC     CREATE TABLE TMPHiPo_Population AS
# MAGIC 	SELECT DISTINCT MSSalesID
# MAGIC 	FROM ocpstaging_smdp_ipcosell.vw_CY_CALCAccountTags AS CCT
# MAGIC 	WHERE lcase(CCT.TagName) Like '%hipo%' 
# MAGIC     

# COMMAND ----------

start("Inserting into table:[TPAccountsDim]")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TPAccountsDim
# MAGIC 	SELECT CAST(DRA.RevenueAccountKey AS INT) AS TPAccountKey
# MAGIC 		  ,CAST(DRA.TPID AS INT) AS TPAccountID
# MAGIC 		  ,replace(DRA.TPName,'','') AS TPAccountName
# MAGIC 		  ,CAST(DRA.SalesTerritoryID AS INT) AS SalesTerritoryID
# MAGIC 		  ,CAST(DRA.SubsidiarySubDistrictId AS INT) AS SubsidiarySubDistrictId
# MAGIC 		  ,GSH.SubsidiarySubDistrictName AS SubsidiarySubDistrictName
# MAGIC 		  ,CASE WHEN lower(DRA.IsMalAccount) = 'true' THEN 'Yes' ELSE 'No' END AS MALFlagName
# MAGIC 		  ,CAST(DRA.VerticalID AS TINYINT) AS VerticalID
# MAGIC 		  ,DRA.VerticalName AS VerticalName
# MAGIC 		  ,CAST(DRA.IndustryID AS tinyint) AS IndustryID
# MAGIC 		  ,DRA.IndustryName AS IndustryName
# MAGIC 		  ,CAST(DRA.SubsidiaryID AS INT) AS SubsidiaryID
# MAGIC 	      ,DRA.SubsidiaryName AS SubsidiaryName
# MAGIC 		  ,CAST(DRA.SubSegmentID AS INT) AS SubSegmentID
# MAGIC 		  ,DRA.SubSegmentName AS SubSegmentName
# MAGIC 		  ,REPLACE(DRA.PlatformStatus,'1','Current') AS Status
# MAGIC 		  ,MPA.SegmentGroup AS SegmentGroup
# MAGIC 		  ,MPA.`HQ/DS`
# MAGIC 		  ,CASE WHEN CCT.MSSalesID IS NOT NULL THEN 'Yes' ELSE 'No'
# MAGIC 		   END AS IsHIPOAccount
# MAGIC       FROM ocpstaging_smdp_ipcosell.DimRevenueAccount AS DRA
# MAGIC       LEFT JOIN ocpstaging_smdp_ipcosell.vw_CY_ManagedTopParentAccounts AS MPA 
# MAGIC       ON DRA.TPID = MPA.MSSalesID
# MAGIC       LEFT JOIN TMPHiPo_Population AS CCT 
# MAGIC       ON DRA.TPID = CCT.MSSalesID
# MAGIC       INNER  JOIN ocpstaging_smdp_ipcosell.DimGeographySubsidiaryHierarchy AS GSH  
# MAGIC       ON DRA.SubsidiarySubDistrictID = GSH.SubsidiarySubDistrictID 
# MAGIC 	  INNER JOIN ocpstaging_smdp_ipcosell.DimSegmentHierarchy AS DSH  
# MAGIC       ON DRA.SubSegmentId = DSH.SubSegmentId       

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TMPHiPo_Population;

# COMMAND ----------

end("Successfully created table: [TPAccountsDim]")

# COMMAND ----------

#end of notebook
end()

# COMMAND ----------

# MAGIC %scala
# MAGIC SetNotebookStatus(NotebookPath)