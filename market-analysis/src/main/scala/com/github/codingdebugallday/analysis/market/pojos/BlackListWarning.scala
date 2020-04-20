package com.github.codingdebugallday.analysis.market.pojos

/**
 * 输出的黑名单报警信息
 */
case class BlackListWarning(userId: Long,
                            adId: Long,
                            msg: String) extends Serializable
