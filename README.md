# SparkSacrilege
Dynamically (re)load python files across the cluster without restarting Spark Jupyter Notebooks

SparkSacrilege lets you edit python imports and deploy those changes to your Spark cluster (executors) without 
restarting your Jupyter Notebook or SparkContext. This is very useful when you're actively exploring data and 
developing code a Jupyter Notebook backed by Spark, need to change library code, and don't want to restart
your Notebook / Spark Context.

This is basically a fancy wrapper around eval, but with a nice interface and proper import scoping. Under the hood it
evaluates the given python file inside a constrained scope, wraps function calls in a lambda, and when deployed to
the cluster pickles up the lambda and the scope.

SparkSacrilege relies on `dill` (`pip install dill`) for pickeling of the functions.

# Sample Usage (from example.ipynb)

```python
from SparkSacrilege import SparkSacrilege
ss = SparkSacrilege("SampleLib")
# SampleLib.py contains a function returnTrue() that returns true.
# For demonstration purposes, returnTrue() depends on other functions 
# in SampleLib.py
```

```python
from pyspark.sql.functions import udf
df = spark.createDataFrame(...)
trueUDF = udf(ss.returnTrue, BooleanType())
df.withColumn("true", trueUDF())
```

Neither `SampleLib.py` nor `SparkSacrilege.py` need to be passed to pyspark via `--py-files`.
