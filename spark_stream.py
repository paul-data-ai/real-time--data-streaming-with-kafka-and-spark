import logging

import os
os.environ["PYSPARK_PYTHON"] = "C:/Users/HP SPECTRE/projects/real-data-streaming-project/venv/Scripts/python.exe"

import sys
print("Python executable:", sys.executable)
print("Python version:", sys.version)


# from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    """
    Create a keyspace in Cassandra if it does not already exist.

    Args:
        session (cassandra.cluster.Session): Cassandra session object used to execute CQL commands.

    Prints:
        A confirmation message when the keyspace is created successfully.
    """

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    """
    Create a table in the Cassandra keyspace if it does not already exist.

    Args:
        session (cassandra.cluster.Session): Cassandra session object used to execute CQL commands.

    Prints:
        A confirmation message when the table is created successfully.
    """
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    """
    Insert user data into the Cassandra table.

    Args:
        session (cassandra.cluster.Session): Cassandra session object used to execute CQL commands.
        **kwargs: Keyword arguments containing user data. Expected keys include 'id', 'first_name', 'last_name',
                  'gender', 'address', 'post_code', 'email', 'username', 'dob', 'registered_date', 'phone', and 'picture'.

    Logs:
        Information about the successful insertion of data or an error if the insertion fails.
    """
    print("Inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    # dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    print(kwargs)

    print(f"User ID: {user_id}")
    print(f"First Name: {first_name}")
    print(f"Last Name: {last_name}")
    print(f"Gender: {gender}")
    print(f"Address: {address}")
    print(f"Postcode: {postcode}")
    print(f"Email: {email}")
    print(f"Username: {username}")
    # print(f"Date of Birth: {dob}")
    print(f"Registered Date: {registered_date}")
    print(f"Phone: {phone}")
    print(f"Picture: {picture}")

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def connect_to_kafka(spark_conn):
    """
    Create a streaming DataFrame from Kafka topic.

    Args:
        spark_conn (SparkSession): The SparkSession object used to read data from Kafka.

    Returns:
        DataFrame: A DataFrame representing the stream of data from the Kafka topic.
    """
    spark_df = None

    try:
        print("Attempting to connect to Kafka...")

        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        print("Kafka DataFrame schema:", spark_df.printSchema())
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")
        print(f"Kafka DataFrame creation failed: {e}")

    return spark_df


def create_spark_connection():
    """
    Create and configure a SparkSession for streaming data from Kafka.

    Returns:
        SparkSession: A SparkSession object configured for the application.

    Logs:
        Information about the successful creation of the Spark session or an error if the session creation fails.
    """
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:sspark-cassandra-connector-assembly_2.12-3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,"
                                           "org.apache.kafka:kafka-clients:3.4.0,"
                                           "org.apache.kafka:kafka-streams-scala_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

            # .config('spark.network.timeout', '800s') \
            # .config('spark.executor.heartbeatInterval', '100s') \
            # .config('spark.rpc.askTimeout', '120s') \
            # .config('spark.driver.memory', '4g') \
            # .config('spark.driver.cores', '2') \
            # .config('spark.rpc.message.maxSize', '128') \
            # .config('spark.eventLog.enabled', 'true') \

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")

    return s_conn


def create_cassandra_connection():
    """
    Create a connection to the Cassandra cluster.

    Returns:
        cassandra.cluster.Session: A session object for interacting with the Cassandra cluster.

    Logs:
        Error message if the connection to Cassandra fails.
    """
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    """
    Transform the raw Kafka DataFrame into a DataFrame with a defined schema.

    Args:
        spark_df (DataFrame): The raw DataFrame from Kafka.

    Returns:
        DataFrame: A DataFrame with data parsed according to the defined schema.

    Prints:
        The transformed DataFrame for verification.
    """
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    if spark_df is not None:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
    else:
        sel = None

    return sel


if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        # print('spark_df', spark_df)
        selection_df = create_selection_df_from_kafka(spark_df)
        print('selection_df', selection_df)
        # selection_df.show(truncate=False)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
