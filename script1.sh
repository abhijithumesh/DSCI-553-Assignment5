export PYSPARK_PYTHON=python3.6
rm task1_res
spark-submit task1.py $ASNLIB/publicdata/business_second.json $ASNLIB/publicdata/business_first.json task1_res