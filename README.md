# UE19CS344-Database-Technologies-Course-Project

## Objective:
<ol>
  <li>Collect twitter feed for a certain time duration for 5 hashtags</li>
  <li>Ingest it into spark</li>
  <li>Use tumbling window of 5 minutes to aggregate count of each of the above hashtags</li>
  <li>Publish them as topics into kafka</li>
  <li>Export that into a Datastore</li>
</ol>

## Technology used
<ol>
  <li>Tweepy to authenticate and retrieve tweets using Twitter API</li>
  <li>Socket streaming to stream it to pyspark</li>
  <li>pyspark structred streaming and window processing to collect on hashtag</li>
  <li>Apache kafka to store these tweets under the respective topic</li>
  <li>Pymongo ORM to export the tweets to Mongodb </li>
</ol>

## File structure
<ol>
  <li>twitter_server.py is the script that streams out twitter feed</li>
  <li>pyspark_client.py is the pyspark script acting as aclient for the streaming server</li>
  <li>kafka_consumer.py is the script that will export data fed into kafka to Mongodb</li>
</ol>

## How to setup and run?
Please follow instr.txt file on setting up the environemnt and running the project.
