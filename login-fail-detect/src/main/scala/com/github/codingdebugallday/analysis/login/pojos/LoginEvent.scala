package com.github.codingdebugallday.analysis.login.pojos

/**
 * 登录事件样例类
 */
case class LoginEvent(userId: Long,
                      ip: String,
                      eventType: String,
                      eventTime: Long) extends Serializable
