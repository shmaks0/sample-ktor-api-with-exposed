package io.shmaks.samples.ktor.service

import io.shmaks.samples.ktor.model.AccountOperation
import io.shmaks.samples.ktor.model.Page
import io.shmaks.samples.ktor.model.XdAccount
import io.shmaks.samples.ktor.model.XdTransfer
import jetbrains.exodus.database.TransientEntityStore
import kotlinx.dnq.query.*
import org.joda.time.DateTime
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.math.BigDecimal

interface TransferService {
    fun createAccount(accountClient: String, accountName: String, accountCurrency: String, initialBalance: BigDecimal = BigDecimal.ZERO): Long

    fun transferMoney(fromAccNumber: Long, toAccNumber: Long, _amount: BigDecimal, currency: String)

    fun getHistory(accNumber: Long, page: Int, pageSize: Int): Page<AccountOperation>
}

class TransferServiceImpl(private val store: TransientEntityStore, private val currencyService: CurrencyService) : TransferService {
    override fun createAccount(accountClient: String, accountName: String, accountCurrency: String, initialBalance: BigDecimal): Long {
        // assume that accountClient is correct

        if (!currencyService.supportCurrency(accountCurrency)) {
            throw IllegalArgumentException("Unsupported currency")
        }

        val now = DateTime()
        return store.transactional {
            XdAccount.new {
                clientId = accountClient
                name = accountName
                accNumber = it.getSequence("accountNumbers").increment()
                currencyCode = accountCurrency
                balance = initialBalance
                createdAt = now
                updatedAt = now
            }.accNumber
        }
    }

    override fun transferMoney(
        fromAccNumber: Long,
        toAccNumber: Long,
        _amount: BigDecimal,
        currency: String
    ) {

        if (!currencyService.supportCurrency(currency)) {
            throw IllegalArgumentException("Unsupported currency")
        }

        if (fromAccNumber == toAccNumber) {
            throw IllegalArgumentException("Should be distinct accounts")
        }

        if (_amount <= BigDecimal.ZERO) {
            throw IllegalArgumentException("Amount should be positive")
        }

        store.transactional {
            val now = DateTime()

            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.firstOrNull()
                ?: throw IllegalArgumentException("Wrong account")
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.firstOrNull()
                ?: throw IllegalArgumentException("Wrong account")

            val currencyPairs = arrayOf(
                CurrencyPair(fromAcc.currencyCode, currency),
                CurrencyPair(currency, toAcc.currencyCode),
                CurrencyPair(currency, fromAcc.currencyCode)
            )
            val exchangeRates = currencyService.exchangeRates(currencyPairs)
            if (exchangeRates.size != 3) {
                throw IllegalArgumentException("Unsupported currency")
            }

            val newBalanceFrom = exchangeRates[0]!! * fromAcc.balance - _amount

            if (newBalanceFrom < BigDecimal.ZERO) {
                throw IllegalStateException("Insufficient funds")
            }

            fromAcc.balance = exchangeRates[2]!! * newBalanceFrom
            toAcc.balance += exchangeRates[1]!! * _amount
            fromAcc.updatedAt = now
            toAcc.updatedAt = now

            XdTransfer.new {
                currencyCode = currency
                amount = _amount
                from = fromAcc
                to = toAcc
                createdAt = now
            }
        }

    }

    override fun getHistory(accNumber: Long, page: Int, pageSize: Int): Page<AccountOperation> =
        store.transactional(readonly = true) {
            val account = XdAccount.filter { acc -> acc.accNumber eq accNumber }.firstOrNull()
                ?: throw IllegalArgumentException("Wrong account")

            val totalCount = XdTransfer
                .filter { (it.from eq account) or (it.to eq account) }
                .size()
            val totalPages = Math.ceil(totalCount.toDouble() / pageSize.toDouble()).toInt()

            val offset = page * pageSize

            var content = emptyList<AccountOperation>()

            if (totalCount > offset) {
                content = XdTransfer
                    .filter { (it.from eq account) or (it.to eq account) }
                    .sortedBy(XdTransfer::createdAt, asc = false)
                    .drop(offset)
                    .take(pageSize)
                    .toList()
                    .map { AccountOperation(
                        dateTime = it.createdAt,
                        amount = if (it.from == account) it.amount.negate() else it.amount,
                        currencyCode = it.currencyCode,
                        contrAccNumber = if (it.from == account) it.to.accNumber else it.from.accNumber
                    ) }

            }

            Page(content = content, page = page, pageSize = pageSize, totalCount = totalCount, totalPages = totalPages)
        }
}
