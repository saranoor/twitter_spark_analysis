<H1>Sentiment analysis on streaming twitter data using Spark Structured Streaming & Python </H1>

This project is a good starting point for those who have little or no experience with <b>Apache Spark Streaming</b>. We use Twitter data since Twitter provides an API for developers that is easy to access.
We present an end-to-end architecture on how to stream data from Twitter, clean it, and apply a trained sentiment analysis model to detect the polarity and subjectivity of each tweet.

<b> Input data:</b> Live tweets with a keyword <br>
<b>Main model:</b> Data preprocessing and apply sentiment analysis on the tweets <br>
<b>Output:</b> A parquet file with all the tweets and their sentiment analysis scores (polarity and subjectivity) <br>

<img align="center"  width="90%" src="https://github.com/saranoor/twitter_spark_analysis/blob/main/architecture.png">

We use Python version 3.7.6 and Spark version 2.4.7. We should be cautious on the versions that we use because different versions of Spark require a different version of Python. 

## Main Libraries
<b> tweepy:</b> interact with the Twitter Streaming API and create a live data streaming pipeline with Twitter <br>
<b> pyspark streaming: </b>preprocess the twitter data (Python's Spark library) <br>
<b> pyspark MLlib:</b> Trained the model using Pyspark ML lib<br>

## Instructions
First, run the <b>Part 1:</b> <i>twitter_connection.py</i> and let it continue running. <br>
Then, run the <b>Part 2:</b> <i>sentiment_analysis.py</i> from a different IDE. 
## Part 0: Create a model pipeline and train a ML model
In this phase, we used data of twitter from Kaggle. We create a pipeline that is comprise of the
following step
0 String to index(convert string target to numeric)
1. Tokenizer
2. Stop Words Remouver
3. CountVectorizer
4. Vector Assember
5. Model Training (Logistric Regression)
6. Index to string
## Part 1: Send tweets from the Twitter API 
In this part, we use our developer credentials to authenticate and connect to the Twitter API. We also create a TCP socket between Twitter's API and Spark, which waits for the call of the Spark Structured Streaming and then sends the Twitter data. Here, we use Python's Tweepy library for connecting and getting the tweets from the Twitter API. 
While sending the data to socket we send message id as well as full text tweet

## Part 2: Tweet preprocessing and sentiment analysis
In this part, we receive the data from the TCP socket and preprocess it with the pyspark library, which is Python's API for Spark. 
Then, we use the model we have trained in Part 0. After sentiment analysis, we save the tweet and the sentiment analysis scores in a parquet file,csv file and also display on console which is a data storage format.



