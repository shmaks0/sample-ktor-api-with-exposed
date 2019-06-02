package io.shmaks.samples.ktor.model

import org.joda.time.DateTime
import java.math.BigDecimal

data class AccountOperation(
    val dateTime: DateTime,
    val amount: BigDecimal,
    val currencyCode: String,
    val contrAccNumber: Long
)
