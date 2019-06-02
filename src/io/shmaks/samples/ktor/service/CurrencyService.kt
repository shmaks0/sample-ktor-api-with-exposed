package io.shmaks.samples.ktor.service

import java.math.BigDecimal

class CurrencyService {

    private val rates = mapOf(
        "USD" to mapOf(
            "EUR" to BigDecimal.valueOf(0.893033),
            "GBP" to BigDecimal.valueOf( 0.791835)
        ),

        "EUR" to mapOf(
            "USD" to BigDecimal.valueOf(1.11978),
            "GBP" to BigDecimal.valueOf( 0.886548)
        ),

        "GBP" to mapOf(
            "USD" to BigDecimal.valueOf(1.26289),
            "EUR" to BigDecimal.valueOf(1.12797)
        )
    )

    fun supportCurrency(code: String) = rates.containsKey(code)

    fun exchangeRates(currencyPairs: Array<CurrencyPair>): Map<Int, BigDecimal> =
        currencyPairs
            .mapIndexed { idx, currencyPair -> Pair(
                    idx,
                    if (currencyPair.from == currencyPair.to) BigDecimal.ONE
                                                         else rates[currencyPair.from]?.get(currencyPair.to)
                )
            }
            .filter { it.second != null }
            .map { it.first to it.second!! }
            .toMap()
}

data class CurrencyPair(val from: String, val to: String)
