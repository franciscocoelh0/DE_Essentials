hdfs dfs -put data/retail_db/departments /user/franciscocoelho/retail_db/departments
hdfs dfs -put data/retail_db/orders /user/franciscocoelho/retail_db/orders
hdfs dfs -put data/retail_db/order_items /user/franciscocoelho/retail_db/order_items
hdfs dfs -put data/retail_db/products /user/franciscocoelho/retail_db/products 

hdfs dfs -ls /user/franciscocoelho/retail_db

hdfs dfs -rm -R skipTrash /user/franciscocoelho/retail_db 

hdfs dfs -ls /user/franciscocoelho/retail_db