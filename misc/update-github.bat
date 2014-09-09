:: Batch Update GitHub
:: by Rodrigo Ferreira

:: This file should be copied to the Eclipse workspace folder

:: This option hides the commands during batch execution
:: Comment the next line putting :: before it in order to debug the program
:: @echo off

:: This command deletes the folder and its files and subfolders
::rd /S /Q C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-datamining\src

:: This command copies the updated files
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-core\src C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-core\src
copy /Y C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-core\pom.xml C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-core\pom.xml

:: This command copies the updated files
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-datamining\src C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-datamining\src
copy /Y C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-datamining\pom.xml C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-datamining\pom.xml

:: This command copies the updated files
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-data\src C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-data\src
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-data\repo C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-data\repo
copy /Y C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-data\pom.xml C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-data\pom.xml

:: This command copies the updated files
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-common\src C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-common\src
copy /Y C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-common\pom.xml C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-common\pom.xml

:: This command copies the updated files
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-geometry\src C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-geometry\src
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-geometry\repo C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-geometry\repo
copy /Y C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-geometry\pom.xml C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-geometry\pom.xml

:: This command copies the updated files
xcopy /Y /S /Q /I C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-operators\src C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-operators\src
copy /Y C:\Users\Rodrigo\Documents\interimage-2\workspace-maven\interimage-operators\pom.xml C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-operators\pom.xml

:: This command copies the updated file
::copy /Y C:\Users\Rodrigo\Documents\interimage-2\s3\scripts\interimage-datamining-import.pig C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-datamining\interimage-datamining-import.pig

:: This command copies the updated file
::copy /Y C:\Users\Rodrigo\Documents\interimage-2\s3\scripts\interimage-data-import.pig C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-data\interimage-data-import.pig

:: This command copies the updated file
::copy /Y C:\Users\Rodrigo\Documents\interimage-2\s3\scripts\interimage-common-import.pig C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-common\interimage-common-import.pig

:: This command copies the updated file
::copy /Y C:\Users\Rodrigo\Documents\interimage-2\s3\scripts\interimage-geometry-import.pig C:\Users\Rodrigo\Documents\GitHub\interimage-2\interimage-geometry\interimage-geometry-import.pig