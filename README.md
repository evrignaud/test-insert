# test-insert

To generate the jar file just run:

    .\gradlew assemble

The jar file is generated inside the `build\libs` folder

Then run the test-insert script like this:

    .\test-insert
    or
    ./test-insert.sh

It will generate the following output (filtering out the warning lines)

    > .\test-insert

    ==> Start Test insert
    ==> 1/ Let MongoDB generate the '_id' column
    ==> Drop the MongoDB TestDB database
    ==> Generating 2000000 of test lines with 50 columns, inside temp file %AppData%\Local\Temp\test-data2561411448025931298.csv
    ==> Save the records into mongodb://localhost:27017/TestDB.test-insert
    ==> saveDataset executed in 54 seconds

    ==> Start Test insert
    ==> 2/ Create data file with '_id'       
    ==> Drop the MongoDB TestDB database
    ==> Generating 2000000 of test lines with 50 columns, inside temp file %AppData%\Local\Temp\test-data12374211472522640010.csv
    ==> Save the records into mongodb://localhost:27017/TestDB.test-insert
    ==> saveDataset executed in 1 minutes, 52 seconds

    ==> Start Test insert
    ==> 3/ Add the '_id' column using Spark
    ==> Drop the MongoDB TestDB database
    ==> Generating 2000000 of test lines with 50 columns, inside temp file %AppData%\Local\Temp\test-data14441304898864433124.csv
    ==> Save the records into mongodb://localhost:27017/TestDB.test-insert
    ==> saveDataset executed in 2 minutes, 1 seconds
