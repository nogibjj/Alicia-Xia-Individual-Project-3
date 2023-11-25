from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# Run a SQL query on a Spark DataFrame and return the result of the query
def query_transform():
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = """
            SELECT p1.Region, p1.Country Name, p1.2015, p1.2016, p1.2017, p1.2018, p2.2019, p2.2020, p2.2021, p2.2022, AVG(p2.2022) OVER(PARTITION BY p1.Region) as avg_2022
            FROM popu_1_delta p1
            JOIN popu_2_delta p2 ON p1.Country Name = p2.Country Name
            ORDER BY avg_2022 DESC, p1.Country Name
        """
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz_1():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please double check.")

    query_result = query.select("Region", "avg_2022").toPandas()

    # Group by 'Region' and calculate the mean of 'avg_year_2022'
    grouped_data = query_result.groupby("Region")["avg_2022"].mean().reset_index()

    # Create a bar chart with the grouped data
    grouped_data.plot(kind="bar", x="Region", y="avg_2022", color="cyan")

    # Set labels and title
    plt.xlabel("Region")
    plt.ylabel("Average Population in 2022")
    plt.title("Average Population in 2022 by Region")

    # Show the plot
    plt.show("Avg_2022_Region.png")

    query_result = query.select("Region", "avg_2022").toPandas()


def viz_2():
    spark = SparkSession.builder.appName("Query").getOrCreate()
    # Write a SQL query to select rows where 'Country' is 'United States' or 'China'
    query = """
    SELECT Country Name, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022 
    FROM popu_1_delta
    WHERE Country IN ('United States', 'China', 'Japan')
    """

    # Execute the SQL query
    query_result = spark.sql(query)

    # Convert the result to a Pandas DataFrame for further operations or visualization
    popu_us_china = query_result.toPandas().set_index("Country Name")

    # Transposing the DataFrame to have years as the X-axis and wages as the Y-axis
    popu_transposed = popu_us_china.transpose()

    # Plotting the line graph
    popu_transposed.plot(kind="line", marker="o")

    # Setting labels and title
    plt.xlabel("Year")
    plt.ylabel("Population")
    plt.title("Development of Population Over the Years for the US, China and Japan")

    # Display the legend
    plt.legend(title="Country")

    # Show the plot
    plt.show("popu_us_china")


if __name__ == "__main__":
    query_transform()
    viz_1()
    viz_2()
