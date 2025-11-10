@echo off
REM start_consumer.bat - launches consumer_delta.py with spark-submit including Kafka & Delta packages
SETLOCAL

REM ----------------- Adjust these if needed -----------------
IF "%SPARK_HOME%"=="" SET "SPARK_HOME=C:\spark-3.3.4-bin-hadoop3"
IF "%JAVA_HOME%"=="" SET "JAVA_HOME=C:\jdk-11.0.28+6"

REM ----------------- Simple .env loader (optional) -----------------
IF EXIST "%~dp0\.env" (
  FOR /F "usebackq tokens=1* delims==" %%A IN ("%~dp0\.env") DO (
    SET "%%A=%%B"
  )
)

REM Ensure SPARK_HOME exists
IF NOT EXIST "%SPARK_HOME%" (
  ECHO SPARK_HOME "%SPARK_HOME%" does not exist. Edit this batch file to correct SPARK_HOME.
  GOTO :EOF
)
SET "PATH=%SPARK_HOME%\bin;%PATH%"

REM Packages (adjust to your Spark/Scala version if necessary)
SET "PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,io.delta:delta-core_2.12:2.1.0"

REM Python runtime notes:
ECHO NOTE: This script expects kafka-python to be installed in the Python environment Spark uses.
ECHO If missing install: pip install kafka-python

REM Run spark-submit (local[*] is convenient for dev; adjust for cluster)
SET "WORKDIR=%~dp0"
SET "PYFILE=%WORKDIR%consumer_delta.py"

ECHO Launching spark-submit...
spark-submit --master local[*] --packages %PACKAGES% "%PYFILE%"

IF %ERRORLEVEL% NEQ 0 (
  ECHO spark-submit exited with error code %ERRORLEVEL%.
) ELSE (
  ECHO consumer exited normally.
)

ENDLOCAL
