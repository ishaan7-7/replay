(.venv) C:\engine_module_pipeline>cd replay

(.venv) C:\engine_module_pipeline\replay>replayer_service.bat
[USAGE] replayer_service.bat "csv_path_optional" source_id
Example: replayer_service.bat sim001
Example: replayer_service.bat "C:\engine_module_pipeline\data\csv\engine_module.csv" sim001

(.venv) C:\engine_module_pipeline\replay>



C:\Users\ishaan256185>cd C:\engine_module_pipeline

C:\engine_module_pipeline>call .venv\Scripts\Activate.bat

(.venv) C:\engine_module_pipeline>cd kafka_consumer

(.venv) C:\engine_module_pipeline\kafka_consumer>start_consumer.bat
NOTE: This script expects kafka-python to be installed in the Python environment Spark uses.
If missing install: pip install kafka-python
Launching spark-submit...
:: loading settings :: url = jar:file:/C:/spark-3.3.4-bin-hadoop3/jars/ivy-2.5.1.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: C:\Users\ishaan256185\.ivy2\cache
The jars for the packages stored in: C:\Users\ishaan256185\.ivy2\jars
org.apache.spark#spark-sql-kafka-0-10_2.12 added as a dependency
io.delta#delta-core_2.12 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-772a37a0-ee6a-466e-bb4d-106dd1be2c42;1.0
        confs: [default]
        found org.apache.spark#spark-sql-kafka-0-10_2.12;3.3.4 in central
        found org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.3.4 in central
        found org.apache.kafka#kafka-clients;2.8.1 in central
        found org.lz4#lz4-java;1.8.0 in central
        found org.xerial.snappy#snappy-java;1.1.8.4 in central
        found org.slf4j#slf4j-api;1.7.32 in central
        found org.apache.hadoop#hadoop-client-runtime;3.3.2 in central
        found org.spark-project.spark#unused;1.0.0 in central
        found org.apache.hadoop#hadoop-client-api;3.3.2 in central
        found commons-logging#commons-logging;1.1.3 in central
        found com.google.code.findbugs#jsr305;3.0.0 in central
        found org.apache.commons#commons-pool2;2.11.1 in central
        found io.delta#delta-core_2.12;2.1.0 in central
        found io.delta#delta-storage;2.1.0 in central
        found org.antlr#antlr4-runtime;4.8 in central
        found org.codehaus.jackson#jackson-core-asl;1.9.13 in central
:: resolution report :: resolve 1153ms :: artifacts dl 75ms
        :: modules in use:
        com.google.code.findbugs#jsr305;3.0.0 from central in [default]
        commons-logging#commons-logging;1.1.3 from central in [default]
        io.delta#delta-core_2.12;2.1.0 from central in [default]
        io.delta#delta-storage;2.1.0 from central in [default]
        org.antlr#antlr4-runtime;4.8 from central in [default]
        org.apache.commons#commons-pool2;2.11.1 from central in [default]
        org.apache.hadoop#hadoop-client-api;3.3.2 from central in [default]
        org.apache.hadoop#hadoop-client-runtime;3.3.2 from central in [default]
        org.apache.kafka#kafka-clients;2.8.1 from central in [default]
        org.apache.spark#spark-sql-kafka-0-10_2.12;3.3.4 from central in [default]
        org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.3.4 from central in [default]
        org.codehaus.jackson#jackson-core-asl;1.9.13 from central in [default]
        org.lz4#lz4-java;1.8.0 from central in [default]
        org.slf4j#slf4j-api;1.7.32 from central in [default]
        org.spark-project.spark#unused;1.0.0 from central in [default]
        org.xerial.snappy#snappy-java;1.1.8.4 from central in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   16  |   0   |   0   |   0   ||   16  |   0   |
        ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-772a37a0-ee6a-466e-bb4d-106dd1be2c42
        confs: [default]
        0 artifacts copied, 16 already retrieved (0kB/24ms)
2025-11-11 01:38:14,414 INFO Prometheus metrics server started on 8001
2025-11-11 01:38:14,416 INFO Starting consumer_delta (group=engine_module_consumer_grp_backfill) BACKFILL_MODE=True
2025-11-11 01:38:14,417 INFO BACKFILL_MODE enabled; attempting to capture topic end offsets (kafka-python required)
2025-11-11 01:38:15,182 INFO Captured end offsets for topic engine_module: {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0, 11: 0}
25/11/11 01:38:15 INFO SparkContext: Running Spark version 3.3.4
25/11/11 01:38:15 INFO ResourceUtils: ==============================================================
25/11/11 01:38:15 INFO ResourceUtils: No custom resources configured for spark.driver.
25/11/11 01:38:15 INFO ResourceUtils: ==============================================================
25/11/11 01:38:15 INFO SparkContext: Submitted application: consumer_delta_engine_module_consumer_grp_backfill
25/11/11 01:38:15 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
25/11/11 01:38:15 INFO ResourceProfile: Limiting resource is cpu
25/11/11 01:38:15 INFO ResourceProfileManager: Added ResourceProfile id: 0
25/11/11 01:38:15 INFO SecurityManager: Changing view acls to: Ishaan256185
25/11/11 01:38:15 INFO SecurityManager: Changing modify acls to: Ishaan256185
25/11/11 01:38:15 INFO SecurityManager: Changing view acls groups to:
25/11/11 01:38:15 INFO SecurityManager: Changing modify acls groups to:
25/11/11 01:38:15 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(Ishaan256185); groups with view permissions: Set(); users  with modify permissions: Set(Ishaan256185); groups with modify permissions: Set()
25/11/11 01:38:18 INFO Utils: Successfully started service 'sparkDriver' on port 58930.
25/11/11 01:38:18 INFO SparkEnv: Registering MapOutputTracker
25/11/11 01:38:18 INFO SparkEnv: Registering BlockManagerMaster
25/11/11 01:38:19 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
25/11/11 01:38:19 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
25/11/11 01:38:19 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/11/11 01:38:19 INFO DiskBlockManager: Created local directory at C:\Users\ishaan256185\AppData\Local\Temp\1\blockmgr-1a7ecd1b-2b3c-410d-9379-259fb63b848b
25/11/11 01:38:19 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
25/11/11 01:38:19 INFO SparkEnv: Registering OutputCommitCoordinator
25/11/11 01:38:19 INFO Utils: Successfully started service 'SparkUI' on port 4040.
25/11/11 01:38:19 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar at spark://view-localhost:58930/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar at spark://view-localhost:58930/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar at spark://view-localhost:58930/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar at spark://view-localhost:58930/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar at spark://view-localhost:58930/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar at spark://view-localhost:58930/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar at spark://view-localhost:58930/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar at spark://view-localhost:58930/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar at spark://view-localhost:58930/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar at spark://view-localhost:58930/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar at spark://view-localhost:58930/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar at spark://view-localhost:58930/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar at spark://view-localhost:58930/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar at spark://view-localhost:58930/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar at spark://view-localhost:58930/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar at spark://view-localhost:58930/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:20 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar
25/11/11 01:38:21 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:21 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-core_2.12-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\io.delta_delta-core_2.12-2.1.0.jar
25/11/11 01:38:22 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:22 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar
25/11/11 01:38:23 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762805295453
25/11/11 01:38:23 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.kafka_kafka-clients-2.8.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.kafka_kafka-clients-2.8.1.jar
25/11/11 01:38:23 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762805295453
25/11/11 01:38:23 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\com.google.code.findbugs_jsr305-3.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\com.google.code.findbugs_jsr305-3.0.0.jar
25/11/11 01:38:23 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762805295453
25/11/11 01:38:23 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.commons_commons-pool2-2.11.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.commons_commons-pool2-2.11.1.jar
25/11/11 01:38:23 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762805295453
25/11/11 01:38:23 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.spark-project.spark_unused-1.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.spark-project.spark_unused-1.0.0.jar
25/11/11 01:38:23 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:23 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar
25/11/11 01:38:24 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762805295453
25/11/11 01:38:24 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.lz4_lz4-java-1.8.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.lz4_lz4-java-1.8.0.jar
25/11/11 01:38:24 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762805295453
25/11/11 01:38:24 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.xerial.snappy_snappy-java-1.1.8.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.xerial.snappy_snappy-java-1.1.8.4.jar
25/11/11 01:38:24 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762805295453
25/11/11 01:38:24 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.slf4j_slf4j-api-1.7.32.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.slf4j_slf4j-api-1.7.32.jar
25/11/11 01:38:24 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:24 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-api-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.hadoop_hadoop-client-api-3.3.2.jar
25/11/11 01:38:24 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar at file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762805295453
25/11/11 01:38:24 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\commons-logging_commons-logging-1.1.3.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\commons-logging_commons-logging-1.1.3.jar
25/11/11 01:38:24 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:24 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-storage-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\io.delta_delta-storage-2.1.0.jar
25/11/11 01:38:25 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762805295453
25/11/11 01:38:25 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.antlr_antlr4-runtime-4.8.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.antlr_antlr4-runtime-4.8.jar
25/11/11 01:38:25 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762805295453
25/11/11 01:38:25 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.codehaus.jackson_jackson-core-asl-1.9.13.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.codehaus.jackson_jackson-core-asl-1.9.13.jar
25/11/11 01:38:25 INFO Executor: Starting executor ID driver on host view-localhost
25/11/11 01:38:25 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
25/11/11 01:38:25 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762805295453
25/11/11 01:38:25 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.kafka_kafka-clients-2.8.1.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.kafka_kafka-clients-2.8.1.jar
25/11/11 01:38:25 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:25 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-api-3.3.2.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.hadoop_hadoop-client-api-3.3.2.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-core_2.12-2.1.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\io.delta_delta-core_2.12-2.1.0.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-storage-2.1.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\io.delta_delta-storage-2.1.0.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.slf4j_slf4j-api-1.7.32.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.slf4j_slf4j-api-1.7.32.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\com.google.code.findbugs_jsr305-3.0.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\com.google.code.findbugs_jsr305-3.0.0.jar
25/11/11 01:38:26 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762805295453
25/11/11 01:38:26 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.antlr_antlr4-runtime-4.8.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.antlr_antlr4-runtime-4.8.jar
25/11/11 01:38:27 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:27 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar
25/11/11 01:38:27 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762805295453
25/11/11 01:38:27 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.spark-project.spark_unused-1.0.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.spark-project.spark_unused-1.0.0.jar
25/11/11 01:38:27 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762805295453
25/11/11 01:38:27 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.xerial.snappy_snappy-java-1.1.8.4.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.xerial.snappy_snappy-java-1.1.8.4.jar
25/11/11 01:38:27 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762805295453
25/11/11 01:38:27 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.lz4_lz4-java-1.8.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.lz4_lz4-java-1.8.0.jar
25/11/11 01:38:27 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762805295453
25/11/11 01:38:27 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.codehaus.jackson_jackson-core-asl-1.9.13.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.codehaus.jackson_jackson-core-asl-1.9.13.jar
25/11/11 01:38:27 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762805295453
25/11/11 01:38:27 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\commons-logging_commons-logging-1.1.3.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\commons-logging_commons-logging-1.1.3.jar
25/11/11 01:38:28 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762805295453
25/11/11 01:38:28 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.commons_commons-pool2-2.11.1.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.commons_commons-pool2-2.11.1.jar
25/11/11 01:38:28 INFO Executor: Fetching spark://view-localhost:58930/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762805295453
25/11/11 01:38:28 INFO TransportClientFactory: Successfully created connection to view-localhost/127.0.0.1:58930 after 69 ms (0 ms spent in bootstraps)
25/11/11 01:38:28 INFO Utils: Fetching spark://view-localhost:58930/jars/org.apache.commons_commons-pool2-2.11.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp1669101721802427883.tmp
25/11/11 01:38:28 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp1669101721802427883.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.commons_commons-pool2-2.11.1.jar
25/11/11 01:38:29 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.apache.commons_commons-pool2-2.11.1.jar to class loader
25/11/11 01:38:29 INFO Executor: Fetching spark://view-localhost:58930/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:29 INFO Utils: Fetching spark://view-localhost:58930/jars/io.delta_delta-core_2.12-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp16157723584423321094.tmp
25/11/11 01:38:29 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp16157723584423321094.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\io.delta_delta-core_2.12-2.1.0.jar
25/11/11 01:38:29 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/io.delta_delta-core_2.12-2.1.0.jar to class loader
25/11/11 01:38:29 INFO Executor: Fetching spark://view-localhost:58930/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762805295453
25/11/11 01:38:29 INFO Utils: Fetching spark://view-localhost:58930/jars/org.lz4_lz4-java-1.8.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp382890500119061983.tmp
25/11/11 01:38:29 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp382890500119061983.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.lz4_lz4-java-1.8.0.jar
25/11/11 01:38:29 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.lz4_lz4-java-1.8.0.jar to class loader
25/11/11 01:38:29 INFO Executor: Fetching spark://view-localhost:58930/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762805295453
25/11/11 01:38:29 INFO Utils: Fetching spark://view-localhost:58930/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp14056189311128763925.tmp
25/11/11 01:38:29 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp14056189311128763925.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.xerial.snappy_snappy-java-1.1.8.4.jar
25/11/11 01:38:29 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.xerial.snappy_snappy-java-1.1.8.4.jar to class loader
25/11/11 01:38:29 INFO Executor: Fetching spark://view-localhost:58930/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762805295453
25/11/11 01:38:29 INFO Utils: Fetching spark://view-localhost:58930/jars/commons-logging_commons-logging-1.1.3.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp12334520471194381507.tmp
25/11/11 01:38:29 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp12334520471194381507.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\commons-logging_commons-logging-1.1.3.jar
25/11/11 01:38:30 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/commons-logging_commons-logging-1.1.3.jar to class loader
25/11/11 01:38:30 INFO Executor: Fetching spark://view-localhost:58930/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762805295453
25/11/11 01:38:30 INFO Utils: Fetching spark://view-localhost:58930/jars/org.apache.kafka_kafka-clients-2.8.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp13535664446902813123.tmp
25/11/11 01:38:30 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp13535664446902813123.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.kafka_kafka-clients-2.8.1.jar
25/11/11 01:38:30 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.apache.kafka_kafka-clients-2.8.1.jar to class loader
25/11/11 01:38:30 INFO Executor: Fetching spark://view-localhost:58930/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762805295453
25/11/11 01:38:30 INFO Utils: Fetching spark://view-localhost:58930/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp1460120068916758379.tmp
25/11/11 01:38:30 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp1460120068916758379.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.codehaus.jackson_jackson-core-asl-1.9.13.jar
25/11/11 01:38:30 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.codehaus.jackson_jackson-core-asl-1.9.13.jar to class loader
25/11/11 01:38:30 INFO Executor: Fetching spark://view-localhost:58930/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762805295453
25/11/11 01:38:30 INFO Utils: Fetching spark://view-localhost:58930/jars/org.slf4j_slf4j-api-1.7.32.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp16334447435717008230.tmp
25/11/11 01:38:30 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp16334447435717008230.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.slf4j_slf4j-api-1.7.32.jar
25/11/11 01:38:30 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.slf4j_slf4j-api-1.7.32.jar to class loader
25/11/11 01:38:30 INFO Executor: Fetching spark://view-localhost:58930/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:30 INFO Utils: Fetching spark://view-localhost:58930/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp14739161691057406750.tmp
25/11/11 01:38:30 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp14739161691057406750.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar
25/11/11 01:38:30 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar to class loader
25/11/11 01:38:30 INFO Executor: Fetching spark://view-localhost:58930/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762805295453
25/11/11 01:38:30 INFO Utils: Fetching spark://view-localhost:58930/jars/org.spark-project.spark_unused-1.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp6767946329568333922.tmp
25/11/11 01:38:30 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp6767946329568333922.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.spark-project.spark_unused-1.0.0.jar
25/11/11 01:38:30 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.spark-project.spark_unused-1.0.0.jar to class loader
25/11/11 01:38:30 INFO Executor: Fetching spark://view-localhost:58930/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762805295453
25/11/11 01:38:30 INFO Utils: Fetching spark://view-localhost:58930/jars/com.google.code.findbugs_jsr305-3.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp8569139153392563394.tmp
25/11/11 01:38:30 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp8569139153392563394.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\com.google.code.findbugs_jsr305-3.0.0.jar
25/11/11 01:38:31 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/com.google.code.findbugs_jsr305-3.0.0.jar to class loader
25/11/11 01:38:31 INFO Executor: Fetching spark://view-localhost:58930/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762805295453
25/11/11 01:38:31 INFO Utils: Fetching spark://view-localhost:58930/jars/io.delta_delta-storage-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp17503209764413223826.tmp
25/11/11 01:38:31 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp17503209764413223826.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\io.delta_delta-storage-2.1.0.jar
25/11/11 01:38:31 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/io.delta_delta-storage-2.1.0.jar to class loader
25/11/11 01:38:31 INFO Executor: Fetching spark://view-localhost:58930/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762805295453
25/11/11 01:38:31 INFO Utils: Fetching spark://view-localhost:58930/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp13639392688846345770.tmp
25/11/11 01:38:31 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp13639392688846345770.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar
25/11/11 01:38:31 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar to class loader
25/11/11 01:38:31 INFO Executor: Fetching spark://view-localhost:58930/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762805295453
25/11/11 01:38:31 INFO Utils: Fetching spark://view-localhost:58930/jars/org.antlr_antlr4-runtime-4.8.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp14002455095786044416.tmp
25/11/11 01:38:31 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp14002455095786044416.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.antlr_antlr4-runtime-4.8.jar
25/11/11 01:38:31 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.antlr_antlr4-runtime-4.8.jar to class loader
25/11/11 01:38:31 INFO Executor: Fetching spark://view-localhost:58930/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:31 INFO Utils: Fetching spark://view-localhost:58930/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp6719181563725406522.tmp
25/11/11 01:38:32 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp6719181563725406522.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar
25/11/11 01:38:32 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar to class loader
25/11/11 01:38:32 INFO Executor: Fetching spark://view-localhost:58930/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762805295453
25/11/11 01:38:32 INFO Utils: Fetching spark://view-localhost:58930/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp5981294500119335836.tmp
25/11/11 01:38:32 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\fetchFileTemp5981294500119335836.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f\userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511\org.apache.hadoop_hadoop-client-api-3.3.2.jar
25/11/11 01:38:32 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-ab14581b-df28-4b6d-a7fe-b100c5fefc1f/userFiles-b3a58289-2d5d-4256-876c-65cd1fb75511/org.apache.hadoop_hadoop-client-api-3.3.2.jar to class loader
25/11/11 01:38:32 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 59029.
25/11/11 01:38:32 INFO NettyBlockTransferService: Server created on view-localhost:59029
25/11/11 01:38:32 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
25/11/11 01:38:32 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, view-localhost, 59029, None)
25/11/11 01:38:32 INFO BlockManagerMasterEndpoint: Registering block manager view-localhost:59029 with 434.4 MiB RAM, BlockManagerId(driver, view-localhost, 59029, None)
25/11/11 01:38:32 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, view-localhost, 59029, None)
25/11/11 01:38:32 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, view-localhost, 59029, None)
2025-11-11 01:38:33,695 INFO Using startingOffsets=earliest; checkpoint=C:\engine_module_pipeline\data\checkpoints\engine_module_consumer_grp_backfill
25/11/11 01:38:35 WARN KafkaSourceProvider: Kafka option 'kafka.group.id' has been set on this query, it is
 not recommended to set this option. This option is unsafe to use since multiple concurrent
 queries or sources using the same group id will interfere with each other as they are part
 of the same consumer group. Restarted queries may also suffer interference from the
 previous run having the same group id. The user should have only one query per group id,
 and/or set the option 'kafka.session.timeout.ms' to be very small so that the Kafka
 consumers from the previous query are marked dead by the Kafka group coordinator before the
 restarted query starts running.

25/11/11 01:38:40 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
25/11/11 01:38:40 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
2025-11-11 01:38:41,306 INFO Starting backfill monitor thread (auto-stop when reaching target offsets).
2025-11-11 01:38:41,307 INFO Backfill marker already present; stopping monitor.
25/11/11 01:38:41 WARN KafkaSourceProvider: Kafka option 'kafka.group.id' has been set on this query, it is
 not recommended to set this option. This option is unsafe to use since multiple concurrent
 queries or sources using the same group id will interfere with each other as they are part
 of the same consumer group. Restarted queries may also suffer interference from the
 previous run having the same group id. The user should have only one query per group id,
 and/or set the option 'kafka.session.timeout.ms' to be very small so that the Kafka
 consumers from the previous query are marked dead by the Kafka group coordinator before the
 restarted query starts running.





C:\Users\ishaan256185>C:\engine_module_pipeline
'C:\engine_module_pipeline' is not recognized as an internal or external command,
operable program or batch file.

C:\Users\ishaan256185>cd C:\engine_module_pipeline

C:\engine_module_pipeline>call .venv\Scripts\Activate.bat

(.venv) C:\engine_module_pipeline>cd kafka_consumer

(.venv) C:\engine_module_pipeline\kafka_consumer>start_consumer.bat
NOTE: This script expects kafka-python to be installed in the Python environment Spark uses.
If missing install: pip install kafka-python
Launching spark-submit...
:: loading settings :: url = jar:file:/C:/spark-3.3.4-bin-hadoop3/jars/ivy-2.5.1.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: C:\Users\ishaan256185\.ivy2\cache
The jars for the packages stored in: C:\Users\ishaan256185\.ivy2\jars
org.apache.spark#spark-sql-kafka-0-10_2.12 added as a dependency
io.delta#delta-core_2.12 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-82b3d59c-7b34-4c07-a244-2435d0dde81d;1.0
        confs: [default]
        found org.apache.spark#spark-sql-kafka-0-10_2.12;3.3.4 in central
        found org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.3.4 in central
        found org.apache.kafka#kafka-clients;2.8.1 in central
        found org.lz4#lz4-java;1.8.0 in central
        found org.xerial.snappy#snappy-java;1.1.8.4 in central
        found org.slf4j#slf4j-api;1.7.32 in central
        found org.apache.hadoop#hadoop-client-runtime;3.3.2 in central
        found org.spark-project.spark#unused;1.0.0 in central
        found org.apache.hadoop#hadoop-client-api;3.3.2 in central
        found commons-logging#commons-logging;1.1.3 in central
        found com.google.code.findbugs#jsr305;3.0.0 in central
        found org.apache.commons#commons-pool2;2.11.1 in central
        found io.delta#delta-core_2.12;2.1.0 in central
        found io.delta#delta-storage;2.1.0 in central
        found org.antlr#antlr4-runtime;4.8 in central
        found org.codehaus.jackson#jackson-core-asl;1.9.13 in central
:: resolution report :: resolve 615ms :: artifacts dl 37ms
        :: modules in use:
        com.google.code.findbugs#jsr305;3.0.0 from central in [default]
        commons-logging#commons-logging;1.1.3 from central in [default]
        io.delta#delta-core_2.12;2.1.0 from central in [default]
        io.delta#delta-storage;2.1.0 from central in [default]
        org.antlr#antlr4-runtime;4.8 from central in [default]
        org.apache.commons#commons-pool2;2.11.1 from central in [default]
        org.apache.hadoop#hadoop-client-api;3.3.2 from central in [default]
        org.apache.hadoop#hadoop-client-runtime;3.3.2 from central in [default]
        org.apache.kafka#kafka-clients;2.8.1 from central in [default]
        org.apache.spark#spark-sql-kafka-0-10_2.12;3.3.4 from central in [default]
        org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.3.4 from central in [default]
        org.codehaus.jackson#jackson-core-asl;1.9.13 from central in [default]
        org.lz4#lz4-java;1.8.0 from central in [default]
        org.slf4j#slf4j-api;1.7.32 from central in [default]
        org.spark-project.spark#unused;1.0.0 from central in [default]
        org.xerial.snappy#snappy-java;1.1.8.4 from central in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   16  |   0   |   0   |   0   ||   16  |   0   |
        ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-82b3d59c-7b34-4c07-a244-2435d0dde81d
        confs: [default]
        0 artifacts copied, 16 already retrieved (0kB/18ms)
2025-11-11 02:36:06,581 INFO Prometheus metrics server started on 8001
2025-11-11 02:36:06,582 INFO Starting consumer_delta (group=engine_module_consumer_grp_backfill) BACKFILL_MODE=True
2025-11-11 02:36:06,583 INFO BACKFILL_MODE enabled; attempting to capture topic end offsets (kafka-python required)
2025-11-11 02:36:07,355 INFO Captured end offsets for topic engine_module: {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0, 11: 0}
25/11/11 02:36:07 INFO SparkContext: Running Spark version 3.3.4
25/11/11 02:36:07 INFO ResourceUtils: ==============================================================
25/11/11 02:36:07 INFO ResourceUtils: No custom resources configured for spark.driver.
25/11/11 02:36:07 INFO ResourceUtils: ==============================================================
25/11/11 02:36:07 INFO SparkContext: Submitted application: consumer_delta_engine_module_consumer_grp_backfill
25/11/11 02:36:07 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
25/11/11 02:36:07 INFO ResourceProfile: Limiting resource is cpu
25/11/11 02:36:07 INFO ResourceProfileManager: Added ResourceProfile id: 0
25/11/11 02:36:07 INFO SecurityManager: Changing view acls to: Ishaan256185
25/11/11 02:36:07 INFO SecurityManager: Changing modify acls to: Ishaan256185
25/11/11 02:36:07 INFO SecurityManager: Changing view acls groups to:
25/11/11 02:36:07 INFO SecurityManager: Changing modify acls groups to:
25/11/11 02:36:07 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(Ishaan256185); groups with view permissions: Set(); users  with modify permissions: Set(Ishaan256185); groups with modify permissions: Set()
25/11/11 02:36:10 INFO Utils: Successfully started service 'sparkDriver' on port 56785.
25/11/11 02:36:10 INFO SparkEnv: Registering MapOutputTracker
25/11/11 02:36:10 INFO SparkEnv: Registering BlockManagerMaster
25/11/11 02:36:10 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
25/11/11 02:36:10 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
25/11/11 02:36:10 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/11/11 02:36:10 INFO DiskBlockManager: Created local directory at C:\Users\ishaan256185\AppData\Local\Temp\1\blockmgr-cb0978fe-918d-40cd-a62f-4cf692ea30e8
25/11/11 02:36:10 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
25/11/11 02:36:10 INFO SparkEnv: Registering OutputCommitCoordinator
25/11/11 02:36:11 INFO Utils: Successfully started service 'SparkUI' on port 4040.
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar at spark://view-localhost:56785/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar at spark://view-localhost:56785/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar at spark://view-localhost:56785/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar at spark://view-localhost:56785/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar at spark://view-localhost:56785/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar at spark://view-localhost:56785/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar at spark://view-localhost:56785/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar at spark://view-localhost:56785/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar at spark://view-localhost:56785/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar at spark://view-localhost:56785/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar at spark://view-localhost:56785/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar at spark://view-localhost:56785/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar at spark://view-localhost:56785/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar at spark://view-localhost:56785/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar at spark://view-localhost:56785/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added JAR file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar at spark://view-localhost:56785/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar
25/11/11 02:36:11 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:11 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-core_2.12-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\io.delta_delta-core_2.12-2.1.0.jar
25/11/11 02:36:12 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:12 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar
25/11/11 02:36:12 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762808767455
25/11/11 02:36:12 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.kafka_kafka-clients-2.8.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.kafka_kafka-clients-2.8.1.jar
25/11/11 02:36:12 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762808767455
25/11/11 02:36:12 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\com.google.code.findbugs_jsr305-3.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\com.google.code.findbugs_jsr305-3.0.0.jar
25/11/11 02:36:12 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762808767455
25/11/11 02:36:12 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.commons_commons-pool2-2.11.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.commons_commons-pool2-2.11.1.jar
25/11/11 02:36:12 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762808767455
25/11/11 02:36:12 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.spark-project.spark_unused-1.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.spark-project.spark_unused-1.0.0.jar
25/11/11 02:36:12 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:12 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar
25/11/11 02:36:13 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762808767455
25/11/11 02:36:13 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.lz4_lz4-java-1.8.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.lz4_lz4-java-1.8.0.jar
25/11/11 02:36:13 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762808767455
25/11/11 02:36:13 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.xerial.snappy_snappy-java-1.1.8.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.xerial.snappy_snappy-java-1.1.8.4.jar
25/11/11 02:36:13 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762808767455
25/11/11 02:36:13 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.slf4j_slf4j-api-1.7.32.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.slf4j_slf4j-api-1.7.32.jar
25/11/11 02:36:13 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:13 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-api-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.hadoop_hadoop-client-api-3.3.2.jar
25/11/11 02:36:13 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar at file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762808767455
25/11/11 02:36:13 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\commons-logging_commons-logging-1.1.3.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\commons-logging_commons-logging-1.1.3.jar
25/11/11 02:36:13 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar at file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:13 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-storage-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\io.delta_delta-storage-2.1.0.jar
25/11/11 02:36:14 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762808767455
25/11/11 02:36:14 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.antlr_antlr4-runtime-4.8.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.antlr_antlr4-runtime-4.8.jar
25/11/11 02:36:14 INFO SparkContext: Added file file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar at file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762808767455
25/11/11 02:36:14 INFO Utils: Copying C:\Users\ishaan256185\.ivy2\jars\org.codehaus.jackson_jackson-core-asl-1.9.13.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.codehaus.jackson_jackson-core-asl-1.9.13.jar
25/11/11 02:36:14 INFO Executor: Starting executor ID driver on host view-localhost
25/11/11 02:36:14 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
25/11/11 02:36:14 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762808767455
25/11/11 02:36:14 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.kafka_kafka-clients-2.8.1.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.kafka_kafka-clients-2.8.1.jar
25/11/11 02:36:15 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:15 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar
25/11/11 02:36:15 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:15 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar
25/11/11 02:36:15 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:15 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-api-3.3.2.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.hadoop_hadoop-client-api-3.3.2.jar
25/11/11 02:36:15 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:15 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-core_2.12-2.1.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\io.delta_delta-core_2.12-2.1.0.jar
25/11/11 02:36:15 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:15 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\io.delta_delta-storage-2.1.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\io.delta_delta-storage-2.1.0.jar
25/11/11 02:36:15 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762808767455
25/11/11 02:36:15 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.slf4j_slf4j-api-1.7.32.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.slf4j_slf4j-api-1.7.32.jar
25/11/11 02:36:16 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762808767455
25/11/11 02:36:16 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\com.google.code.findbugs_jsr305-3.0.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\com.google.code.findbugs_jsr305-3.0.0.jar
25/11/11 02:36:16 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762808767455
25/11/11 02:36:16 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.antlr_antlr4-runtime-4.8.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.antlr_antlr4-runtime-4.8.jar
25/11/11 02:36:16 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:16 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar
25/11/11 02:36:16 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762808767455
25/11/11 02:36:16 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.spark-project.spark_unused-1.0.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.spark-project.spark_unused-1.0.0.jar
25/11/11 02:36:18 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762808767455
25/11/11 02:36:18 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.xerial.snappy_snappy-java-1.1.8.4.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.xerial.snappy_snappy-java-1.1.8.4.jar
25/11/11 02:36:18 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762808767455
25/11/11 02:36:18 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.lz4_lz4-java-1.8.0.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.lz4_lz4-java-1.8.0.jar
25/11/11 02:36:19 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762808767455
25/11/11 02:36:19 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.codehaus.jackson_jackson-core-asl-1.9.13.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.codehaus.jackson_jackson-core-asl-1.9.13.jar
25/11/11 02:36:19 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762808767455
25/11/11 02:36:19 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\commons-logging_commons-logging-1.1.3.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\commons-logging_commons-logging-1.1.3.jar
25/11/11 02:36:19 INFO Executor: Fetching file:///C:/Users/ishaan256185/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762808767455
25/11/11 02:36:19 INFO Utils: C:\Users\ishaan256185\.ivy2\jars\org.apache.commons_commons-pool2-2.11.1.jar has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.commons_commons-pool2-2.11.1.jar
25/11/11 02:36:19 INFO Executor: Fetching spark://view-localhost:56785/jars/org.apache.commons_commons-pool2-2.11.1.jar with timestamp 1762808767455
25/11/11 02:36:19 INFO TransportClientFactory: Successfully created connection to view-localhost/127.0.0.1:56785 after 66 ms (0 ms spent in bootstraps)
25/11/11 02:36:19 INFO Utils: Fetching spark://view-localhost:56785/jars/org.apache.commons_commons-pool2-2.11.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp3508854329381428272.tmp
25/11/11 02:36:19 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp3508854329381428272.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.commons_commons-pool2-2.11.1.jar
25/11/11 02:36:20 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.apache.commons_commons-pool2-2.11.1.jar to class loader
25/11/11 02:36:20 INFO Executor: Fetching spark://view-localhost:56785/jars/io.delta_delta-core_2.12-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:20 INFO Utils: Fetching spark://view-localhost:56785/jars/io.delta_delta-core_2.12-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp10951900741771157203.tmp
25/11/11 02:36:20 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp10951900741771157203.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\io.delta_delta-core_2.12-2.1.0.jar
25/11/11 02:36:20 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/io.delta_delta-core_2.12-2.1.0.jar to class loader
25/11/11 02:36:20 INFO Executor: Fetching spark://view-localhost:56785/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1762808767455
25/11/11 02:36:20 INFO Utils: Fetching spark://view-localhost:56785/jars/org.spark-project.spark_unused-1.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp6890488721430964905.tmp
25/11/11 02:36:20 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp6890488721430964905.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.spark-project.spark_unused-1.0.0.jar
25/11/11 02:36:20 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.spark-project.spark_unused-1.0.0.jar to class loader
25/11/11 02:36:20 INFO Executor: Fetching spark://view-localhost:56785/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1762808767455
25/11/11 02:36:20 INFO Utils: Fetching spark://view-localhost:56785/jars/com.google.code.findbugs_jsr305-3.0.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp3527185502236653092.tmp
25/11/11 02:36:20 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp3527185502236653092.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\com.google.code.findbugs_jsr305-3.0.0.jar
25/11/11 02:36:20 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/com.google.code.findbugs_jsr305-3.0.0.jar to class loader
25/11/11 02:36:20 INFO Executor: Fetching spark://view-localhost:56785/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1762808767455
25/11/11 02:36:20 INFO Utils: Fetching spark://view-localhost:56785/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp11802387395108780681.tmp
25/11/11 02:36:20 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp11802387395108780681.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.xerial.snappy_snappy-java-1.1.8.4.jar
25/11/11 02:36:20 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.xerial.snappy_snappy-java-1.1.8.4.jar to class loader
25/11/11 02:36:20 INFO Executor: Fetching spark://view-localhost:56785/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:20 INFO Utils: Fetching spark://view-localhost:56785/jars/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp2404406936724294833.tmp
25/11/11 02:36:21 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp2404406936724294833.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.hadoop_hadoop-client-runtime-3.3.2.jar
25/11/11 02:36:21 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.apache.hadoop_hadoop-client-runtime-3.3.2.jar to class loader
25/11/11 02:36:21 INFO Executor: Fetching spark://view-localhost:56785/jars/io.delta_delta-storage-2.1.0.jar with timestamp 1762808767455
25/11/11 02:36:21 INFO Utils: Fetching spark://view-localhost:56785/jars/io.delta_delta-storage-2.1.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp8713256302467641724.tmp
25/11/11 02:36:21 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp8713256302467641724.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\io.delta_delta-storage-2.1.0.jar
25/11/11 02:36:21 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/io.delta_delta-storage-2.1.0.jar to class loader
25/11/11 02:36:21 INFO Executor: Fetching spark://view-localhost:56785/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:21 INFO Utils: Fetching spark://view-localhost:56785/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp3572942816506758811.tmp
25/11/11 02:36:21 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp3572942816506758811.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar
25/11/11 02:36:21 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.apache.spark_spark-sql-kafka-0-10_2.12-3.3.4.jar to class loader
25/11/11 02:36:21 INFO Executor: Fetching spark://view-localhost:56785/jars/org.slf4j_slf4j-api-1.7.32.jar with timestamp 1762808767455
25/11/11 02:36:21 INFO Utils: Fetching spark://view-localhost:56785/jars/org.slf4j_slf4j-api-1.7.32.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp6575187898629993918.tmp
25/11/11 02:36:21 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp6575187898629993918.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.slf4j_slf4j-api-1.7.32.jar
25/11/11 02:36:21 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.slf4j_slf4j-api-1.7.32.jar to class loader
25/11/11 02:36:21 INFO Executor: Fetching spark://view-localhost:56785/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1762808767455
25/11/11 02:36:21 INFO Utils: Fetching spark://view-localhost:56785/jars/commons-logging_commons-logging-1.1.3.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp9421278810611036645.tmp
25/11/11 02:36:21 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp9421278810611036645.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\commons-logging_commons-logging-1.1.3.jar
25/11/11 02:36:21 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/commons-logging_commons-logging-1.1.3.jar to class loader
25/11/11 02:36:21 INFO Executor: Fetching spark://view-localhost:56785/jars/org.antlr_antlr4-runtime-4.8.jar with timestamp 1762808767455
25/11/11 02:36:21 INFO Utils: Fetching spark://view-localhost:56785/jars/org.antlr_antlr4-runtime-4.8.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp11084484086315626669.tmp
25/11/11 02:36:21 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp11084484086315626669.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.antlr_antlr4-runtime-4.8.jar
25/11/11 02:36:22 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.antlr_antlr4-runtime-4.8.jar to class loader
25/11/11 02:36:22 INFO Executor: Fetching spark://view-localhost:56785/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar with timestamp 1762808767455
25/11/11 02:36:22 INFO Utils: Fetching spark://view-localhost:56785/jars/org.apache.hadoop_hadoop-client-api-3.3.2.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp11703605990454423965.tmp
25/11/11 02:36:22 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp11703605990454423965.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.hadoop_hadoop-client-api-3.3.2.jar
25/11/11 02:36:22 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.apache.hadoop_hadoop-client-api-3.3.2.jar to class loader
25/11/11 02:36:22 INFO Executor: Fetching spark://view-localhost:56785/jars/org.apache.kafka_kafka-clients-2.8.1.jar with timestamp 1762808767455
25/11/11 02:36:22 INFO Utils: Fetching spark://view-localhost:56785/jars/org.apache.kafka_kafka-clients-2.8.1.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp557435444988719772.tmp
25/11/11 02:36:22 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp557435444988719772.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.kafka_kafka-clients-2.8.1.jar
25/11/11 02:36:22 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.apache.kafka_kafka-clients-2.8.1.jar to class loader
25/11/11 02:36:22 INFO Executor: Fetching spark://view-localhost:56785/jars/org.lz4_lz4-java-1.8.0.jar with timestamp 1762808767455
25/11/11 02:36:22 INFO Utils: Fetching spark://view-localhost:56785/jars/org.lz4_lz4-java-1.8.0.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp6660661134432081328.tmp
25/11/11 02:36:22 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp6660661134432081328.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.lz4_lz4-java-1.8.0.jar
25/11/11 02:36:22 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.lz4_lz4-java-1.8.0.jar to class loader
25/11/11 02:36:22 INFO Executor: Fetching spark://view-localhost:56785/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar with timestamp 1762808767455
25/11/11 02:36:22 INFO Utils: Fetching spark://view-localhost:56785/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp10457468163731886724.tmp
25/11/11 02:36:22 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp10457468163731886724.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.codehaus.jackson_jackson-core-asl-1.9.13.jar
25/11/11 02:36:23 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.codehaus.jackson_jackson-core-asl-1.9.13.jar to class loader
25/11/11 02:36:23 INFO Executor: Fetching spark://view-localhost:56785/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar with timestamp 1762808767455
25/11/11 02:36:23 INFO Utils: Fetching spark://view-localhost:56785/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp12944106419043171007.tmp
25/11/11 02:36:23 INFO Utils: C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\fetchFileTemp12944106419043171007.tmp has been previously copied to C:\Users\ishaan256185\AppData\Local\Temp\1\spark-501df7de-83a9-497d-a39c-efdd6b31a894\userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158\org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar
25/11/11 02:36:23 INFO Executor: Adding file:/C:/Users/ishaan256185/AppData/Local/Temp/1/spark-501df7de-83a9-497d-a39c-efdd6b31a894/userFiles-0fdca67c-3b64-4f1a-9fef-9c9665c5d158/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.4.jar to class loader
25/11/11 02:36:23 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 56871.
25/11/11 02:36:23 INFO NettyBlockTransferService: Server created on view-localhost:56871
25/11/11 02:36:23 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
25/11/11 02:36:23 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, view-localhost, 56871, None)
25/11/11 02:36:23 INFO BlockManagerMasterEndpoint: Registering block manager view-localhost:56871 with 434.4 MiB RAM, BlockManagerId(driver, view-localhost, 56871, None)
25/11/11 02:36:23 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, view-localhost, 56871, None)
25/11/11 02:36:23 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, view-localhost, 56871, None)
2025-11-11 02:36:24,282 INFO Using startingOffsets=earliest; checkpoint=C:\engine_module_pipeline\data\checkpoints\engine_module_consumer_grp_backfill
25/11/11 02:36:26 WARN KafkaSourceProvider: Kafka option 'kafka.group.id' has been set on this query, it is
 not recommended to set this option. This option is unsafe to use since multiple concurrent
 queries or sources using the same group id will interfere with each other as they are part
 of the same consumer group. Restarted queries may also suffer interference from the
 previous run having the same group id. The user should have only one query per group id,
 and/or set the option 'kafka.session.timeout.ms' to be very small so that the Kafka
 consumers from the previous query are marked dead by the Kafka group coordinator before the
 restarted query starts running.

25/11/11 02:36:30 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
25/11/11 02:36:31 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
2025-11-11 02:36:32,383 INFO Starting backfill monitor thread (auto-stop when reaching target offsets).
25/11/11 02:36:32 WARN KafkaSourceProvider: Kafka option 'kafka.group.id' has been set on this query, it is
 not recommended to set this option. This option is unsafe to use since multiple concurrent
 queries or sources using the same group id will interfere with each other as they are part
 of the same consumer group. Restarted queries may also suffer interference from the
 previous run having the same group id. The user should have only one query per group id,
 and/or set the option 'kafka.session.timeout.ms' to be very small so that the Kafka
 consumers from the previous query are marked dead by the Kafka group coordinator before the
 restarted query starts running.

2025-11-11 02:36:37,550 INFO Processing batch 0
2025-11-11 02:36:37,744 INFO Batch 0 empty - nothing to do
2025-11-11 02:36:37,744 INFO Finished processing batch 0 in 0.194s
25/11/11 02:36:38 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 6012 milliseconds















# Project root
PROJECT_ROOT=C:\engine_module_pipeline
VENV_ACTIVATE=C:\engine_module_pipeline\.venv\Scripts\activate.bat
PYTHON_EXE=C:\engine_module_pipeline\.venv\Scripts\python.exe
FALLBACK_PYTHON=C:\Users\ishaan256185\AppData\Local\Programs\Python\Python311\python.exe

# CSV (replayer)
REPLAYER_DEFAULT_CSV=C:\engine_module_pipeline\data\csv\engine_module.csv

# Paths
LOG_DIR=C:\engine_module_pipeline\logs
DLQ_DIR=C:\engine_module_pipeline\dlq
CHECKPOINT_DIR=C:\engine_module_pipeline\data\checkpoints
DELTA_BASE=C:\engine_module_pipeline\delta
DELTA_TABLE_PATH=C:\engine_module_pipeline\delta\engine_module_delta
DATA_RAW=C:\engine_module_pipeline\data\raw
SPARK_LOCAL_DIR=C:\engine_module_pipeline\tmp\spark_local

# Kafka
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=engine_module
KAFKA_PARTITIONS=12
KAFKA_KEY_FIELD=source_id

# Consumer group / backfill
CONSUMER_GROUP=engine_module_consumer_grp_backfill
BACKFILL_MODE=true

# Batch settings
BATCH_SIZE=500
BATCH_MAX_WAIT_S=5.0

# Java / Spark
JAVA_HOME=C:\jdk-11.0.28+6
PYSPARK_PYTHON=C:\engine_module_pipeline\.venv\Scripts\python.exe
PYSPARK_DRIVER_PYTHON=C:\engine_module_pipeline\.venv\Scripts\python.exe
SPARK_LOCAL_IP=127.0.0.1
SPARK_HOME=C:\spark-3.3.4-bin-hadoop3


# Observability / misc
PROMETHEUS_PORT=8001
LOG_LEVEL=INFO

KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=engine_module
CONSUMER_GROUP=engine_module_backfill_fresh_v2
BACKFILL_MODE=true
PROMETHEUS_PORT=8001
MLFLOW_TRACKING_URI=file:///C:/engine_module_pipeline/mlruns







C:\engine_module_pipeline>call .venv\Scripts\Activate.bat

(.venv) C:\engine_module_pipeline>uvicorn fastapi_ingest.app.main:app --host 127.0.0.1 --port 8000 --reload



(.venv) C:\engine_module_pipeline>python replay\replayer.py ^
More?   --csv data\csv\engine_module.csv ^
More?   --source-id sim001 ^
More?   --features config\features.json ^
More?   --batch-size 50 ^
More?   --mode time_scaled ^
More?   --max-rps 5






