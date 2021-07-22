@echo off

set scriptPath=%~dp0

"%JAVA_HOME%/bin/java" -jar %scriptPath%\build\libs\insert-0.0.1-SNAPSHOT.jar 2000000 50

"%JAVA_HOME%/bin/java" -jar %scriptPath%\build\libs\insert-0.0.1-SNAPSHOT.jar 2000000 50 create_data_file_with_id

"%JAVA_HOME%/bin/java" -jar %scriptPath%\build\libs\insert-0.0.1-SNAPSHOT.jar 2000000 50 add_id_using_spark

set exitcode=%errorlevel%
endlocal & exit /b %exitcode%
