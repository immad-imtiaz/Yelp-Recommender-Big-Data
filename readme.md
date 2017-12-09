# Yelp New Business Helper - Big Data Miners
Team: Ravi Bisla, Immad Imtiaz, Sharif-ul-Islam


## Getting Started
According to Bloomberg, 8 out of 10 business fail within the first 18 months. A whopping 80% crash and burn. For any new business that needs to open on a particular place it is necessary to find you future competitors (other similar business in that area) and strategize based on the performance of those business.

In this project we build a web-based business analytics solution  that helps new business strategize using Yelp dataset. Our web based analytics answers the following question for any new business.

### What are the similar businesses (competitors) around the area we want to open our new business? 
Helps to decide whether the place we have chosen for our new business is good or not or whether the vicinity of this place is already saturated with similar businesses.

### How these future competitors are performing? 
This helps to analyze whether the nature of business we want to open would be successful at the location we have chosen or not.

### What is the expected customer traffic on each day of peak and each hour of day? 
This helps the new business to manage staff and other resources based on the expected customer traffic so that they can utilize these resources in an optimal way.

### Areas where other similar businesses are doing good and areas where these business are lacking? 
This helps the new business benchmark their quality and service accordingly. The new business can do better on the areas where all these businesses are lacking and capitalize. They can also know their minimum quality and service benchmarks based on where these similar business are doing well.

### What the users are thinking? 
What the users are thinking about a particular business is very important. A business may learn about user reactions, what they like and dislike and use that knowledge to make informed decision. We try to provide a solution for this as well.  


### Setting up the project for TA

1) Clone the reporsitory on the cluster and create a virtual environment for the project. (On the cluster because the precomputed data is on cassandra on the cluster)

2) In yelp_spark/settings.py change the cassandra cluster to CASSANDRA_SERVERS = ['199.60.17.171', '199.60.17.188']

3) Activate virtual environment

4) Run the engine
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 server.py

5) On your local for tunnelling
ssh -L 5432:gateway.sfucloud.ca:5432 iimtiaz@gateway.sfucloud.ca -N

6) On your brower you can access the application entering localhost:5432




