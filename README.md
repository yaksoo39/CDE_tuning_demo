# CDE_tuning_demo

The Cloudera Data Engineering job **Analysis** features provide consolidated views of stage dependency, scheduling and performance metrics for each run of your application.  Additional information can be found in the [Cloudera documentation](https://docs.cloudera.com/data-engineering/cloud/troubleshooting/topics/cde-deep-analysis.html) and also [this blog post](https://blog.cloudera.com/demystifying-spark-jobs-to-optimize-for-cost-and-performance/).

This repository provides a simple, standalone application and a step-by-step walk-though of troubleshooting and tuning for it using CDE job Analysis features.

## Setup
The application was tested on a virtual cluster with 32 cores and 128GB max resources (node size = 8 core / 32GB).  It's also assumed that you are:
1.  familiar with the CDE CLI (see the [CDE_CLI_demo](https://github.com/curtishoward/CDE_CLI_demo) for examples)
2.  able to create tables under the Hive ```default``` database

We'll first create a resource with the application files each job will use:
```
cde resource create --name tuning-demo-files
cde resource upload --local-path demo_table_gen.py --name tuning-demo-files 
cde resource upload --local-path etl_job.py --name tuning-demo-files 
```

Next, we'll create and run the ```gen_table``` job to create a 3G table that will be used in our example (created Hive table: ```default.tuning_demo_input_table```):
```
cde job create --application-file /app/mount/demo_table_gen.py --name gen_table \
    --executor-cores 1 --executor-memory 8G --type spark --mount-1-resource tuning-demo-files
cde job run --name gen_table
```

We can now create the ```etl_job``` application that we'll troubleshoot and tune:
```
cde job create --application-file /app/mount/etl_job.py --name etl_job --executor-cores 1 \
    --executor-memory 1G --type spark --mount-1-resource tuning-demo-files
```

Next, enable the CDE Analysis option can be enabled from the Job's Configuration Edit page:
![cde_da](cde_da.png)

Finally, run the job:
```
cde job run --name etl_job 
```

As curently configured, the job should fail. 

## CDE Job Analysis
Some common approaches to identify the root cause of the job failure would be to review the Spark History UI or the driver and executor logs for the application.  For small, simple jobs this could be a reasonable approach; however, for more complex applications (especially those you may be unfamiliar with), finding the source of a problem can be a tedious process of clicking through multiple levels of the Spark History UI or of reviewing potentially very large logs which combine logging events from all tasks and stages over the timeline of the application.


### Stage View
The Analysis tab consolidates Spark stage information for the job.  Understanding the dependencies, order, and component of the overall execution time between these stages from the Spark Histort and logs can be non-trivial.  The CDE Analysisview makes it immediately obvious which stages took the longest or were dependencies of others, potentially highligting bottlenecks.  The color coding also summarizes the component of time spend waiting for resources versus executing, per stage:
![stage_dag](stage_dag.png)

### Stage Drill-Down
Selecting a particular stage from the high level CDE job Analysis view will show a summary graph of input and output sizes for that stage.  Using this view, it's obvious that there is skew in the sort stage of our sample application:
![drill_down](drill_down.png)

Moving down to the memory allocation and utilization graph for the same stage's 'drill-down' view, we can also see  memory utilization likely exceeded the 1GB that we specified per executor (allocated completely to this task, since cores per executor was set to 1):
![memory_over](memory_over.png)

#### Address the Problem
Some options to resolve the issue of skewed data would be to use a [salt](), higher parallelism using repartition, or potentially Spark 3's [AQE](https://blog.cloudera.com/how-does-apache-spark-3-0-increase-the-performance-of-your-sql-workloads/).  For the purposes of this example, we will simply increase the executor memory from 1GB to 8GB:
```
cde job update --name etl_job --executor-memory 8G
```

### Tuning
With 8GB per executor, the job should now complete successfully.  We can now inspect the same graphs of memory utilization over time to quickly select an appropriate executor memory setting that will both allow the job to run reliably (with some headroom for data growth) and at the same time minimize use of unnecessary resources (translating to optimal costs to run the job).  In this case, we should be able to safely lower the executor memory setting to 4GB:
![memory_tune](memory_tune.png)

### Deep Analysis
Running CDE's Deep Analysis option for the job run provides another level of detail for each stage, in the form of a flame graph summarizing time spent at each level of the call stack:
![flame](flame.png)

In our case, the time spent within a Python context (the BUSY_FUNC UDF function defined in the code) is a small fraction of the stage 1 execution time.  And of the 3 stages, stage 2 dominated the overall execution time and resources requirements.  So overall, stage 1 is less of a concern.  Still, we could replace the UDF function with an equivalent SparkSQL-only expression:
```
#spark.sql('''SELECT *, BUSY_FUNC(`r`) as r_func
spark.sql('''SELECT *, POWER(`r`, 3) as r_func,
```
This is a best practice, as it eliminates the (potentially high cost) of serialization between JVM and Python contexts that is required to support a Python UDF of the type that we tested.


## Summary
To summarize, CDE's Analysis features enhance the existing tools available to data engineers for troubleshooting and tuning of Spark applications:
1. **Productivity**:  CDE job Analysis summarizes key performance metrics such as memory, IO, and CPU over time, provides a consolidated view of the stages and their relationships, as well as a 'deep analysis' that profiles specific calls and libraries.  Together, these tools allow the developer to quickly isolate stability and performance issues, tightening the development lifecycle and freeing developers' time for other tasks.
2. **Cost and Performance**:  Aggregate views of resource utilization over the timeline of the application let you confidently and accurately choose job configurations that balance stability with resource utilization and cost.
