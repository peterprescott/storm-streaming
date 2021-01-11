# Big Data Streaming with Storm 

## COMP529 Assignment 2

We are required to create a Storm topology that reads tweets related to the coronavirus.

More specifically, the topology should have:
- a spout that produces a stream of tweets,
- a bolt to filter irrelevant words,
- a bolt to determine negative words, and another to determine positive words,
- and 'a score bolt to decide if the incoming tweets are negative or positive'.


## Similar Projects

[Prime Numbers](https://computerscience360.wordpress.com/2016/03/28/creating-a-project-in-apache-storm-crunching-prime-numbers/)

[Storm Wordcount using Python](https://github.com/Azure-Samples/hdinsight-python-storm-wordcount)

[PVillard's Twitter Streaming Topology](https://github.com/pvillard31/storm-twitter)

[David Kiss's Twitter Streaming Topology](https://github.com/davidkiss/storm-twitter-word-count)

[Dockerized Storm Topology](https://medium.com/free-code-camp/apache-storm-is-awesome-this-is-why-you-should-be-using-it-d7c37519a427)

[Twitter Sentiments Real-Time Analyzer](https://github.com/dgreenshtein/storm-twitter-sentiments)

[Another Sentiment Analysis Topology](https://github.com/qiozas/sentiment-analysis-storm)

## Run Dockerized Standalone Cluster

```
git clone https://github.com/pi-prescott/storm-streaming
cp storm-streaming
docker-compose up
```

## Generate Maven Project, Inject Storm Code, Package Jar, and run in Local Mode

```
./generate
cd example
./copy_code.sh
cd ../quickstart
./package_and_run.sh
```




