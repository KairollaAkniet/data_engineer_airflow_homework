from pyspark.sql import functions as F


def load_data(spark, path):
    return spark.read.csv(str(path), header=True, inferSchema=True)


def clean_data(df):
    return df.withColumn(
        "known_for_department",
        F.when(F.col("known_for_department") == "", F.lit(None))
        .otherwise(F.col("known_for_department")),
    ).na.drop(subset=["name", "known_for_department"])


def decode_gender(df):
    return df.withColumn(
        "gender_label",
        F.when(F.col("gender") == 0, "Unknown")
        .when(F.col("gender") == 1, "Female")
        .when(F.col("gender") == 2, "Male")
        .when(F.col("gender") == 3, "Other")
        .otherwise("Unknown"),
    )


def add_popularity_tier(df):
    return df.withColumn(
        "popularity_tier",
        F.when(F.col("popularity") >= 40, "A-list")
        .when(F.col("popularity") >= 20, "B-list")
        .when(F.col("popularity") >= 10, "C-list")
        .otherwise("Unknown"),
    )
