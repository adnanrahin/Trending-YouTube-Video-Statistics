## Step: 1 Download The Data Set
1. [DataSet](https://www.kaggle.com/datasets/datasnaek/youtube-new/code) Download The Data-Set from here.
2. Once the Data Set is Downloaded, Drop All the Json file to `category_ids` Directory.
3. And Put All the CSV file to `videos_info` directory.

## Step 2: Project Build and Run Instruction: Data-Pre-Processing-Engine
1. This project Multi-Module Maven Project.
2. Module [data-preprocessing-engine](https://github.com/adnanrahin/Trending-YouTube-Video-Statistics/tree/master/data-preprocessing-engine) Pre-Process the Data that was Downloaded in Step 1.
3. Go to Project Root Directory and Run `mvn clean install`, to build the project and run the unit tests.
4. Once the Build is Done is Run `TrendVideoDataPreProcessor` this scripts, and it will Generate `videos_info_filter` Directory with new Transformed Data.
5. Then Run `TrendVideoCategoryPreProcessor` and this scripts will parse all the JSON data, will generate `video_category`

## Step 3: Run All the scoring Spark Queries: Data-Scoring-Processor
1. The Program start From `VideoScoringProcessor` scripts.
2. This Scripts takes three arguments from the Command Line.
3. * args(0) -> videos_info_filter that was generated in step 2.
   * args(1) -> video_category that was generated in step 2.
   * args(2) -> Output Root Directory where spark job write.

## Spark Standalone Cluster Configuration:
To the see the spark-standalone cluster configuration check `.run` directory.


### Docker Container Commands and Instructions:

1. Use this command to build docker image `docker build -t cluster-apache-spark:3.0.2 .`
2. Once the docker image build is done, run command `docker-compose up`.

```/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode cluster --class org.youtube.trending.VideoScoringProcessor --name VideoScoringPorcessor --driver-memory 1G --executor-memory 1G --executor-cores 2 --total-executor-cores 1 /opt/spark-apps-scoring/data-scoring-processor-1.0-SNAPSHOT.jar /opt/spark-data/trending_youtube_video_statistics_dataset/videos_info_filter/* /opt/spark-data/trending_youtube_video_statistics_dataset/video_category/* /opt/spark-data/trending_youtube_video_statistics_dataset```
