# StreamingKafkaFilter
Using Kafka to filter streaming data received with flume.We minimize large json lines to get only the selected fields. In this case we will use as source my repository FlumeTwitterSource.

# Requirements
We only need to start the twitter kafka Flume following the steps in my repository "FlumeTwitterSource" https://github.com/gonzalobd/FlumeTwitterSource


# Features
Flume twitter source give us large json lines with a los of fields and information about the tweet (contributions, answers, profile info...) into a Kafka topic "twitter". In this case we make a filter to select only a few fields, like the text, language, location, number of "favs", retweets, etc. 
This filtered data will be sent to a new Kafka topic called "twitter_filtered"

# Running
After running the FlumeTwitterSource we just have to run the class twitterClass.java in our favourite IDE.
To see the results we can open a Kafka console consumer. In our kafka folder:

       > bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_filtered --from-beginning
