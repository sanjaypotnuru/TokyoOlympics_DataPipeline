# Databricks notebook source
spark

# COMMAND ----------

spark.conf.get("spark.master")

# COMMAND ----------

storage_account = "tokyoolympicsdatastorage"
application_id = "b7be9d7a-0c13-4ffc-896b-58b36ea66d22"
directory_id = "83df00aa-66c4-4617-ab43-9d443a121247"
service_credential = "agA8Q~GmtDa9sqf9FIEpzq1iAaYN4mL2WjIStbul"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{directory_id}/oauth2/v2.0/token")


# COMMAND ----------

spark.conf.set("fs.azure.account.key.tokyoolympicsdatastorage.dfs.core.windows.net", "4BaKtV5d6Nz0AogPC4tzSlKfvdO7CTwSE3HRAOi6OV1ndWE31CbuYgCBMnZ4zlX6zN3GjfTZhPNI+ASttDJgFQ==")
csv_path = "abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/bronze/Medals.csv"

df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path))
display(df)

# COMMAND ----------

base_path = "abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/bronze/"

Athletes_path = base_path + "Athletes.csv"
Coaches_path = base_path + "Coaches.csv"
EntriesGender_path = base_path + "EntriesGender.csv"
Medals_path = base_path + "Medals.csv"
Teams_path = base_path + "Teams.csv"

# COMMAND ----------

Athletes_path = spark.read.format("csv").option("header","true").load(Athletes_path)
Coaches_path  = spark.read.format("csv").option("header","true").load(Coaches_path )
EntriesGender_path = spark.read.format("csv").option("header","true").load(EntriesGender_path)
Medals_path  = spark.read.format("csv").option("header","true").load(Medals_path )
Teams_path = spark.read.format("csv").option("header","true").load(Teams_path)

# COMMAND ----------

Athletes_path.printSchema()

# COMMAND ----------

display(Athletes_path)

# COMMAND ----------

Coaches_path.printSchema()

# COMMAND ----------

display(Coaches_path)

# COMMAND ----------

EntriesGender_path.printSchema()

# COMMAND ----------


display(EntriesGender_path)

# COMMAND ----------

Medals_path.printSchema()

# COMMAND ----------

display(Medals_path)

# COMMAND ----------

Teams_path.printSchema()

# COMMAND ----------

display(Teams_path)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# COMMAND ----------

def trim_all(df):
    return df.select([F.trim(F.col(c)).alias(c) for c in df.columns])


def uppercase(df, cols):
    for c in cols:
        df = df.withColumn(c, F.upper(F.col(c)))
    return df


def standardize_country(df, colname="Country"):
    mapping = {
        "UNITED STATES": "USA",
        "US": "USA",
        "U.S.A": "USA",
        "GREAT BRITAIN": "GBR",
        "RUSSIA": "RUS",
        "REPUBLIC OF KOREA": "KOR",
        "SOUTH KOREA": "KOR",
        "JAPAN": "JPN"

    }

    mapping_expr = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
    return df.withColumn(colname, F.coalesce(mapping_expr[F.col(colname)], F.col(colname)))


# COMMAND ----------

def data_quality_report(df):
    print("Row Count:", df.count())

    print("\nNull Counts:")
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

    print("\nDuplicate Count:")
    print(df.count(), "rows;", df.distinct().count(), "distinct")

    print("\nSample:")
    df.show(5)


# COMMAND ----------

for df in [Coaches_path, Athletes_path, EntriesGender_path, Medals_path, Teams_path]:
    data_quality_report(df)


# COMMAND ----------

'''Coaches (Silver)'''
coaches = trim_all(Coaches_path)

coaches = uppercase(coaches, ["Country", "Discipline", "Event"])
coaches = standardize_country(coaches, "Country")

coaches = coaches.dropDuplicates()


# COMMAND ----------

'''Athletes (Silver)'''

athletes = trim_all(Athletes_path)

athletes = uppercase(athletes, ["Country", "Discipline"])
athletes = standardize_country(athletes, "Country")

athletes = athletes.dropDuplicates()


# COMMAND ----------

'''EntriesGender (Silver)'''

entries = trim_all(EntriesGender_path)

# cast gender counts to int
entries = entries \
    .withColumn("Female", F.col("Female").cast("int")) \
    .withColumn("Male", F.col("Male").cast("int")) \
    .withColumn("Total", F.col("Total").cast("int"))

entries = uppercase(entries, ["Discipline"])

# Validating totals
entries = entries.withColumn(
    "TotalCheck", F.when(F.col("Female")+F.col("Male") == F.col("Total"), F.lit("OK")).otherwise("Mismatch")
)

entries = entries.dropDuplicates()


# COMMAND ----------

'''Medals (Silver)'''

medals = trim_all(Medals_path)

num_cols = ["Rank", "Gold", "Silver", "Bronze", "Total", "Rank by Total"]
for c in num_cols:
    medals = medals.withColumn(c, F.col(c).cast("int"))

medals = uppercase(medals, ["TeamCountry"])
medals = standardize_country(medals, "TeamCountry")

medals = medals.dropDuplicates()


# COMMAND ----------

'''Teams (Silver)'''

teams = trim_all(Teams_path)

teams = uppercase(teams, ["TeamName", "Country", "Discipline", "Event"])
teams = standardize_country(teams, "Country")

teams = teams.dropDuplicates()


# COMMAND ----------

'''CROSS DATASET VALIDATION'''

'''✔ Countries must match across tables'''

from pyspark.sql.functions import col

invalid_countries = (
    coaches.select(col("Country")).subtract(athletes.select(col("Country")))
)

invalid_countries.show()


# COMMAND ----------

'''✔ Discipline mismatch check'''

discipline_mismatch = (
    athletes.select("Discipline")
    .subtract(entries.select("Discipline"))
)
discipline_mismatch.show()


# COMMAND ----------

'''✔ Teams must match athletes' country'''

teams_invalid = teams.join(
    athletes.select("Country").distinct(),
    on="Country",
    how="left_anti"
)
teams_invalid.show()


# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = EntriesGender_path.withColumn(
    'Avg_Female', EntriesGender_path['Female'] / EntriesGender_path['Total']
).withColumn(
    'Avg_Male', EntriesGender_path['Male'] / EntriesGender_path['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

coaches.write.mode("overwrite").parquet("abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/silver/coaches")
athletes.write.mode("overwrite").parquet("abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/silver/athletes")
entries.write.mode("overwrite").parquet("abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/silver/entries")
medals.write.mode("overwrite").parquet("abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/silver/medals")
teams.write.mode("overwrite").parquet("abfss://olympics-datastorage@tokyoolympicsdatastorage.dfs.core.windows.net/silver/teams")

# COMMAND ----------

