from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

BOOTSTRAP_SERVERS = 'confluent-local-broker-1:50459'
TOPIC = 'wikimedia_events'


def output_to_console(df):
    df.writeStream.outputMode('complete').format('console').start()


def main():
    spark = SparkSession.builder.appName('StructuredStreamingKafka').getOrCreate()

    kafka_stream_df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)
        .option('subscribe', TOPIC)
        .load()
    )

    schema = StructType([
        StructField('timestamp', IntegerType()),
        StructField('bot', BooleanType()),
        StructField('minor', BooleanType()),
        StructField('user', StringType()),
        StructField('meta', StructType([
            StructField('domain', StringType())
        ])),
        StructField('length', StructType([
            StructField('old', IntegerType()),
            StructField('new', IntegerType())
        ]))
    ])

    df = kafka_stream_df.select(F.col('value').cast('string'))
    df = df.select(F.from_json(df.value, schema).alias('data'))
    df = df.select('data.timestamp',
                   'data.bot',
                   'data.minor',
                   'data.user',
                   'data.meta.domain',
                   'data.length.old',
                   'data.length.new'
                   )

    df = df.withColumn('length_diff', F.col('new') - F.col('old'))
    df = df.withColumn('length_diff_percent',
                       F.round((((F.col('new') - F.col('old')) / F.col('old')) * 100), 2)
                       )

    # commented out to save memory

    # output_to_console(df)

    # top_domain = df.groupBy('domain').count().orderBy(F.desc('count')).limit(5)
    # output_to_console(top_domain)

    # top_users = df.groupBy('user').agg(F.sum('length_diff').alias('added_sum')).orderBy(F.desc('added_sum')).limit(5)
    # output_to_console(top_users)

    # calc_df = df.agg(
    #     F.count('timestamp').alias('total_count'),
    #     F.round(((F.count_if(F.col('bot') == True) / F.count('bot')) * 100), 2).alias('percent_bot'),
    #     F.round(F.mean('length_diff'), 2).alias('average_length_diff'),
    #     F.min('length_diff').alias('min_length_diff'),
    #     F.max('length_diff').alias('max_length_diff')
    # )
    # output_to_console(calc_df)


    (
        df.writeStream
        .outputMode('append')
        .option('checkpointLocation', 'output')
        .format('csv')
        .option('path', 'output/wikimedia_events.csv')
        .option('header', True)
        .trigger(processingTime='10 seconds')
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
