package com.github.codingdebugallday.analysis.market.pojos

/**
 * 按照省份统计输出结果
 */
case class AdCountByProvince(windowStart: String,
                             windowEnd: String,
                             province: String,
                             count: Long) extends Serializable
