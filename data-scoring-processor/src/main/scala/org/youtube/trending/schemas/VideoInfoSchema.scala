package org.youtube.trending.schemas

case class VideoInfoSchema(
                            videoID: String,
                            trendingDate: String,
                            title: String,
                            channelTitle: String,
                            categoryId: String,
                            publishTime: String,
                            tags: String,
                            views: Long,
                            likes: Long,
                            dislikes: Long,
                            commentCount: Long,
                            thumbnailLink: String,
                            commentsDisable: Boolean,
                            ratingsDisable: Boolean,
                            videoErrorOrRemoved: Boolean,
                            description: String,
                            countryCode: String,
                            countryCategoryCode: String
                          )
