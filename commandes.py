from email._header_value_parser import Terminal

cd "C:\Users\sirin\Documents\ING1_GM\S2\Electifs\Spark__Big__Data"


Terminal 1 :
python simulator.py


Terminal 2:
cd "C:\Users\sirin\Documents\ING1_GM\S2\Electifs\Spark__Big__Data"
$env:HADOOP_HOME="C:\hadoop"
$env:PATH = "C:\hadoop\bin;" + $env:PATH
$env:PYSPARK_PYTHON="C:\Users\sirin\AppData\Local\Programs\Python\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON="C:\Users\sirin\AppData\Local\Programs\Python\Python311\python.exe"
spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.11.0 main_streaming.py



Terminal 3 :
cd "C:\Users\sirin\Documents\ING1_GM\S2\Electifs\Spark__Big__Data"
python dashboard.py