# Big Data Streaming with Storm 

This repo contains the work done for the second assignment done for the
module COMP529 *Big Data Analysis*, done as part of my [Data Analysis
MSc](https://github.com/peterprescott/data-analysis-msc).

We were required to create a Storm topology that reads tweets related to the coronavirus.

More specifically, the topology was required to have:
- a spout that produces a stream of tweets,
- a bolt to filter irrelevant words,
- a bolt to determine negative words, and another to determine positive words,
- and 'a score bolt to decide if the incoming tweets are negative or positive'.

Java code can be found in the [`task/java`](task/java) folder. If you want to run this
yourself, you will need to add your own configuration credentials into
the `TwitterSpout.java` folder.

If you're interested in the report I submitted, you can read that
[here](report/PPrescott_COMP529_Assignment2.pdf). 

