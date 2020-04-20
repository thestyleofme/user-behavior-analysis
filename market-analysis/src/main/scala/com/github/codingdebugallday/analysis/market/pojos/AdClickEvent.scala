package com.github.codingdebugallday.analysis.market.pojos

/**
 * 广告点击事件
 */
case class AdClickEvent(userId: Long,
                        adId: Long,
                        province: String,
                        city: String,
                        timestamp: Long) extends Serializable
