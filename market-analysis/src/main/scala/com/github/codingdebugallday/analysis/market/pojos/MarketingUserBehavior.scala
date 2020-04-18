package com.github.codingdebugallday.analysis.market.pojos

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/19 0:25
 * @since 1.0
 */
case class MarketingUserBehavior(userId: String,
                                 behavior: String,
                                 channel: String,
                                 timestamp: Long) extends Serializable
