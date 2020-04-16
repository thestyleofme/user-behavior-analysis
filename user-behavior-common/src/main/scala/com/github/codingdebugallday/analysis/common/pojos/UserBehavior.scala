package com.github.codingdebugallday.analysis.common.pojos

/**
 * <p>
 * 输入数据样例类
 * </p>
 *
 * @author isacc 2020/04/13 15:41
 * @since 1.0
 */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long) extends Serializable
