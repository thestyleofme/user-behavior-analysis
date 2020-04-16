package com.github.codingdebugallday.analysis.hotitems.pojos

/**
 * <p>
 * 窗口聚合样例类
 * </p>
 *
 * @author isacc 2020/04/13 15:46
 * @since 1.0
 */
case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long) extends Serializable
