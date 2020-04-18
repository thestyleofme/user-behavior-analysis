package com.github.codingdebugallday.analysis.market.pojos

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/19 0:58
 * @since 1.0
 */
case class MarketingViewCount(windowStart: String,
                              windowEnd: String,
                              channel: String,
                              behavior: String,
                              count: Long) extends Serializable
