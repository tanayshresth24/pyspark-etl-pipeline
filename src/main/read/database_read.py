class DatabaseReader:

    def __init__(self, url, properties):
        self.url = url

        # Defensive enforcement of JDBC driver
        self.properties = properties.copy()
        self.properties["driver"] = "com.mysql.cj.jdbc.Driver"

    def create_dataframe(self, spark, database_name, table_name):
        full_table_name = f"{database_name}.{table_name}"

        return spark.read.jdbc(
            url=self.url,
            table=full_table_name,
            properties=self.properties
        )
