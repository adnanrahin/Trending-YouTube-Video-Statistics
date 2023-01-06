package org.youtube.trending.schemas

case class VideoCategoryIdSchema(
                                  etag: String,
                                  kind: String,
                                  itemsEtag: String,
                                  itemsId: String,
                                  itemsKind: String,
                                  itemSnippetsAssignable: Boolean,
                                  itemSnippetsChannelId: String,
                                  itemSnippetsTittle: String,
                                  countryCode: String,
                                  countryCategoryCode: String
                                )
