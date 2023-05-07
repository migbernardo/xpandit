# Miguel Bernardo: Spark 2 Recruitment Challenge
## 1. Walkthrough
### 1.1 General assumptions
- Since an exact format specification of App names was not asked, I decided to use names without symbols and spaces to simplify all the requested data transformations in both datasets. The **parser** function is responsible for making a regex transformation on the strings e.g. "CarMax â€“" &rarr; "CarMax".
- Incorrect readings of the .csv files was avoided by using " as escape characters according to [stack overflow](https://stackoverflow.com/questions/40413526/reading-csv-files-with-quoted-fields-containing-embedded-commas) on the **csvReader** function.
### 1.2 Caveats
- On both datasets we can find instances of app names like "C Programming" and "C++ Programming". Thus, to avoid incorrect aggregation of these different apps, the parser function replaces "++" symbols with "PlusPlus".
- There's one record on the googleplaystore.csv file which doesn't contain a Category value. It was removed from this dataset whenever it was used to make a specific dataframe.
![d566c1ece1db6182d477f75d086c2501.png](/_resources/d566c1ece1db6182d477f75d086c2501.png)

### 1.3 Parts
Part 1 result (df_1)

![ffcd155e416082bbad8f1b276cb1374a.png](/_resources/ffcd155e416082bbad8f1b276cb1374a.png)

Number of records: 1071

Part 2 result (df_2)

![96b97de45e49f578ebc791a55cc137c2.png](/_resources/96b97de45e49f578ebc791a55cc137c2.png)

Number of records: 6274
- Duplicate App names were removed
- Ratings with NaN values were dropped 
- .csv file saved with header

Part 3 result (df_3)

![3cfc908670c6512b168109d4c23a7c23.png](/_resources/3cfc908670c6512b168109d4c23a7c23.png)

Number of records: 8176
- Duplicate App names were removed
- App names with empty strings were removed
- Ratings with NaN values were dropped 
- $ sign from Prices was removed before converting to double format
- M letter was removed from Size before converting to double format
- "DPSTATUS" substring was removed from Current_Version for one specific record (caveat)

Part 4 result (df_3)

![4837d7c30b12be3450b66f9474b6a732.png](/_resources/4837d7c30b12be3450b66f9474b6a732.png)

Number of records: 8176 \
Number of null records (Average_Sentiment_Polarity): 7157 \
Diff: 1019

Part 5 result (df_4)

![d756412c1d1018fca6a73da0cb3e69b4.png](/_resources/d756412c1d1018fca6a73da0cb3e69b4.png)

Number of records: 53
- Average_Sentiment_Polarity was added upon aggregation

## 2. Environment
- OS: Windows 10.0.19043.2006
- IDE: Intellij IDEA 
- Maven archetype: net.alchim31.maven:scala-archetype-simple
- Scala: 2.12
- Spark: 3.4.0
