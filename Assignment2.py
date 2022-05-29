from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType

spark = SparkSession \
    .builder \
    .appName("Assignment2") \
    .getOrCreate()


test = 's3://comp5349-2022/test.json'
train_seperate_questions = 's3://comp5349-2022/train_separate_questions.json'
CUDAv1 = 's3://comp5349-2022/CUADv1.json'

df = spark.read.json(path=test, multiLine=True)
df.show(10)
df.printSchema()

#Create the function that can read the nested json file into spark dataframe.  
from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_nested_json(df):
    column_list = []

    for column_name in df.schema.names:
        # Checking column type is ArrayType
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df

read_nested_json_flag = True

while read_nested_json_flag:
  print("Reading Nested JSON File ... ")
  df = read_nested_json(df)
  read_nested_json_flag = False

  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True

df.show(15, False)

#Create segment function that segments each contract into fixed size(4096 bytes) of segments 
def segmentation(element):
  window = 4096
  stride = 2048
  
  final = []
  for i in element.split():
    final.append(i)
  final2 = ''.join(final)
  final2 = [final2[i : i + stride] for i in range(0, len(final2), window)]
  return final2

  #Get answer start position 
def get_answer_end(start, element):
  return start + len(element)


df2 = df.rdd.map(lambda x: (segmentation(x.context), x.question, x.answer_start, get_answer_end(x.answer_start, x.text)))

df2.take(10)

spark.end()
