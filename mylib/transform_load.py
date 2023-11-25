from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


def load(
    dataset="dbfs:/FileStore/Alicia-Xia-Individual-Project-3/Population_2015-2018.csv",
    dataset2="dbfs:/FileStore/Alicia-Xia-Individual-Project-3/Population_2019-2022.csv",
):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema
    popu_1_df = spark.read.csv(dataset, header=True, inferSchema=True)
    popu_2_df = spark.read.csv(dataset2, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    popu_1_df = popu_1_df.withColumn("id", monotonically_increasing_id())
    popu_2_df = popu_2_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it
    popu_1_df.write.format("delta").mode("overwrite").saveAsTable("popu_1_delta")
    popu_2_df.write.format("delta").mode("overwrite").saveAsTable("popu_2_delta")

    num_rows = popu_1_df.count()
    print(num_rows)
    num_rows = popu_2_df.count()
    print(num_rows)

    return "Completed transform and load"


if __name__ == "__main__":
    load()
