import sys

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from datetime import datetime


def format_date(dt):
    """it is used to convert y-m-d format to d/m/y format."""
    return datetime.strftime(datetime.strptime(dt, '%Y-%m-%d'), '%d/%m/%Y')


def read_header(header_path):
    """it is used to read the header and return a dictionary."""

    header_df = spark.read.csv(header_path).toDF(*['colname', 'offset', 'datattype'])
    list_header = map(lambda row: row.asDict(), header_df.collect())
    return list_header


def extract(lines):
    """it is used to convert fixed text to csv format based on header configuration
	  1. Enclose string if it contains separator
	  2. in case the format of the file is not correct, the program should fail but say explicitly
	  3. date (format yyyy-mm-dd)
	  4. Add CR at the end of the rows and output formar would add newline at the end.
	"""
    separator = ','
    out = []
    header = []
    ##add header
    try:
        for meta in rdd_b.value:
            header.append(meta['colname'])
        out.append(separator.join(header))
        csv_row = []
        for line in lines:
            seek = 0
            for meta in rdd_b.value:
                f_line = line[seek:seek + int(meta['offset'])].strip()
                if (meta['datattype'] == 'date'):
                    csv_row.append(format_date(f_line))
                elif (separator in f_line):
                    csv_row.append('"{}"'.format(f_line))
                else:
                    csv_row.append(f_line)
                seek += int(meta['offset'])
            out.append(separator.join(csv_row) + "\r")
            csv_row = []
    except ValueError:
        raise ValueError("unable to parse the date field value: " + f_line)
    except Exception as error:
        raise Exception('Unknown Exception while parsing file' + repr(error))
    return out


if __name__ == "__main__":
    if (len(sys.argv) < 3):
        print("Please pass the   header file, actual input  and output path in arguments")
        print("Example spark-submit fixedcsv_converter.py <headerfile path> <input folder or file> <outputfolder>")
    else:
        """ FileOutputCommitter to avoid direct committer issue which is deprecated in spark 2.0"""
        conf = SparkConf()
        conf.set("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        sc = spark.sparkContext
        print("header file : " + sys.argv[1])
        """ distrbute the header to all the executor on read only mode """
        rdd_b = sc.broadcast(read_header(sys.argv[1]))
        print("input path : " + sys.argv[2])
        rdd = sc.textFile(sys.argv[2])
        out = rdd.mapPartitions(extract)
        print("output path" + sys.argv[3])
        out.saveAsTextFile(sys.argv[3])
        print("completed")



