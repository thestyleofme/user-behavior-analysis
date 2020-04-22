package com.github.codingdebugallday.analysis.login.pojos

case class LoginWarning(userId: Long,
                        firstFailTime: Long,
                        lastFailTime: Long,
                        warningMsg: String) extends Serializable
