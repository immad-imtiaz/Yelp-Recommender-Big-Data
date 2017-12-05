1) Activate virtual environment on cluster _yelp_recommendation

2) Run the engine
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 server.py

3) On your local for tunnelling
ssh -L 5432:gateway.sfucloud.ca:5432 iimtiaz@gateway.sfucloud.ca -N

4) On your browser just hit and you will get the ui
localhost:5432
