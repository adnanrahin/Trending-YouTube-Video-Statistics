package org.youtube.trending.scoring.schemas

case class VideoCategoryIdSchema(
                                  etag: String,
                                  kind: String,
                                  itemsEtag: String,
                                  itemsId: String,
                                  itemsKind: String,
                                  itemSnippetsAssignable: Boolean,
                                  itemSnippetsChannelId: String,
                                  itemSnippetsTable: String,
                                  countryCode: String,
                                  countryCategoryCode: String
                                )
