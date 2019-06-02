package io.shmaks.samples.ktor.service

import io.shmaks.samples.ktor.model.*
import org.h2.jdbcx.JdbcDataSource
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.math.BigDecimal

interface TransferService {
    fun createAccount(accountClient: String, accountName: String, accountCurrency: String, initialBalance: BigDecimal = BigDecimal.ZERO): Long

    fun transferMoney(fromAccNumber: Long, toAccNumber: Long, _amount: BigDecimal, currency: String)

    fun getHistory(accNumber: Long, page: Int, pageSize: Int): Page<AccountOperation>
}

//data class Account(val id: Long, val accNumber: Long)

class TransferServiceImpl2(private val ds: JdbcDataSource, private val currencyService: CurrencyService) : TransferService {
    override fun createAccount(accountClient: String, accountName: String, accountCurrency: String, initialBalance: BigDecimal): Long {
        // assume that accountClient is correct

        if (!currencyService.supportCurrency(accountCurrency)) {
            throw IllegalArgumentException("Unsupported currency")
        }

        val now = DateTime()

        Database.connect(ds)

        return transaction {
            val inserted = Accounts.insert {
                it[clientId] = accountClient
                it[name] = accountName
                it[currencyCode] = accountCurrency
                it[balance] = initialBalance
                it[createdAt] = now
                it[updatedAt] = now
            }

//            Account(id = inserted[Accounts.id].value, accNumber = inserted[Accounts.accNumber]
            inserted[Accounts.accNumber]
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

        val now = DateTime()

        Database.connect(ds)

        transaction {
            val fromAcc = Accounts.slice(Accounts.id, Accounts.balance, Accounts.currencyCode)
                .select { Accounts.accNumber eq fromAccNumber }
                .firstOrNull()
                ?: throw IllegalArgumentException("Wrong account")

            val toAcc = Accounts.slice(Accounts.id, Accounts.balance, Accounts.currencyCode)
                .select { Accounts.accNumber eq toAccNumber }
                .firstOrNull()
                ?: throw IllegalArgumentException("Wrong account")

            val currencyPairs = arrayOf(
                CurrencyPair(fromAcc[Accounts.currencyCode], currency),
                CurrencyPair(currency, toAcc[Accounts.currencyCode]),
                CurrencyPair(currency, fromAcc[Accounts.currencyCode])
            )
            val exchangeRates = currencyService.exchangeRates(currencyPairs)
            if (exchangeRates.size != 3) {
                throw IllegalArgumentException("Unsupported currency")
            }

            val newBalanceFrom = exchangeRates[0]!! * fromAcc[Accounts.balance] - _amount

            if (newBalanceFrom < BigDecimal.ZERO) {
                throw IllegalStateException("Insufficient funds")
            }

            val withdrawn =
                Accounts.update({ (Accounts.id eq fromAcc[Accounts.id]) and (Accounts.balance eq fromAcc[Accounts.balance]) }) {
                    it[balance] = exchangeRates[2]!! * newBalanceFrom
                    it[updatedAt] = now
                }

            if (withdrawn != 1) {
                rollback()
                throw ConcurrentModificationException()
            }

            val debited = Accounts.update ({ (Accounts.id eq toAcc[Accounts.id]) and (Accounts.balance eq toAcc[Accounts.balance]) }) {
                it[balance] = toAcc[Accounts.balance] + exchangeRates[1]!! * _amount
                it[updatedAt] = now
            }

            if (debited != 1) {
                rollback()
                throw ConcurrentModificationException()
            }

            Transfers.insert {
                it[from] = fromAcc[Accounts.id]
                it[to] = toAcc[Accounts.id]
                it[currencyCode] = currency
                it[amount] = _amount
                it[createdAt] = now
            }
        }
    }

    override fun getHistory(accNumber: Long, page: Int, pageSize: Int): Page<AccountOperation> {

        Database.connect(ds)

        return transaction {
            val account = Accounts.select { Accounts.accNumber eq accNumber }
                .firstOrNull()
                ?: throw IllegalArgumentException("Wrong account")

            val accountId = account[Accounts.id]
            val totalCount = Transfers
                .select{ (Transfers.from eq accountId) or (Transfers.to eq accountId) }
                .count()
            val totalPages = Math.ceil(totalCount.toDouble() / pageSize.toDouble()).toInt()

            val offset = page * pageSize

            var content = emptyList<AccountOperation>()

            if (totalCount > offset) {
                val fromAlias = Accounts.alias("from")
                val toAlias = Accounts.alias("to")

                content = Join(
                    Join(Transfers, fromAlias, additionalConstraint = { fromAlias[Accounts.id] eq Transfers.from }),
                    toAlias,
                    additionalConstraint = { toAlias[Accounts.id] eq Transfers.to }
                ).select{ (Transfers.from eq account[Accounts.id]) or (Transfers.to eq account[Accounts.id]) }
                    .orderBy(Transfers.createdAt to SortOrder.DESC)
                    .limit(pageSize, offset = offset)
                    .map { AccountOperation(
                        dateTime = it[Transfers.createdAt],
                        amount = if (it[Transfers.from] == accountId) it[Transfers.amount].negate() else it[Transfers.amount],
                        currencyCode = it[Transfers.currencyCode],
                        contrAccNumber = if (it[Transfers.from] == accountId) it[toAlias[Accounts.accNumber]] else it[fromAlias[Accounts.accNumber]]
                    ) }
            }

            Page(content = content, page = page, pageSize = pageSize, totalCount = totalCount, totalPages = totalPages)


        }
    }
}

//class TransferServiceImpl(private val store: TransientEntityStore, private val currencyService: CurrencyService) : TransferService {
//    override fun createAccount(accountClient: String, accountName: String, accountCurrency: String, initialBalance: BigDecimal): Long {
//        // assume that accountClient is correct
//
//        if (!currencyService.supportCurrency(accountCurrency)) {
//            throw IllegalArgumentException("Unsupported currency")
//        }
//
//        val now = DateTime()
//        return store.transactional {
//            XdAccount.new {
//                clientId = accountClient
//                name = accountName
//                accNumber = it.getSequence("accountNumbers").increment()
//                currencyCode = accountCurrency
//                balance = initialBalance
//                createdAt = now
//                updatedAt = now
//            }.accNumber
//        }
//    }
//
//    override fun transferMoney(
//        fromAccNumber: Long,
//        toAccNumber: Long,
//        _amount: BigDecimal,
//        currency: String
//    ) {
//
//        if (!currencyService.supportCurrency(currency)) {
//            throw IllegalArgumentException("Unsupported currency")
//        }
//
//        if (fromAccNumber == toAccNumber) {
//            throw IllegalArgumentException("Should be distinct accounts")
//        }
//
//        if (_amount <= BigDecimal.ZERO) {
//            throw IllegalArgumentException("Amount should be positive")
//        }
//
//        store.transactional {
//            val now = DateTime()
//
//            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.firstOrNull()
//                ?: throw IllegalArgumentException("Wrong account")
//            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.firstOrNull()
//                ?: throw IllegalArgumentException("Wrong account")
//
//            val currencyPairs = arrayOf(
//                CurrencyPair(fromAcc.currencyCode, currency),
//                CurrencyPair(currency, toAcc.currencyCode),
//                CurrencyPair(currency, fromAcc.currencyCode)
//            )
//            val exchangeRates = currencyService.exchangeRates(currencyPairs)
//            if (exchangeRates.size != 3) {
//                throw IllegalArgumentException("Unsupported currency")
//            }
//
//            val newBalanceFrom = exchangeRates[0]!! * fromAcc.balance - _amount
//
//            if (newBalanceFrom < BigDecimal.ZERO) {
//                throw IllegalStateException("Insufficient funds")
//            }
//
//            fromAcc.balance = exchangeRates[2]!! * newBalanceFrom
//            toAcc.balance += exchangeRates[1]!! * _amount
//            fromAcc.updatedAt = now
//            toAcc.updatedAt = now
//
//            XdTransfer.new {
//                currencyCode = currency
//                amount = _amount
//                from = fromAcc
//                to = toAcc
//                createdAt = now
//            }
//        }
//
//    }
//
//    override fun getHistory(accNumber: Long, page: Int, pageSize: Int): Page<AccountOperation> =
//        store.transactional(readonly = true) {
//            val account = XdAccount.filter { acc -> acc.accNumber eq accNumber }.firstOrNull()
//                ?: throw IllegalArgumentException("Wrong account")
//
//            val totalCount = XdTransfer
//                .filter { (it.from eq account) or (it.to eq account) }
//                .size()
//            val totalPages = Math.ceil(totalCount.toDouble() / pageSize.toDouble()).toInt()
//
//            val offset = page * pageSize
//
//            var content = emptyList<AccountOperation>()
//
//            if (totalCount > offset) {
//                content = XdTransfer
//                    .filter { (it.from eq account) or (it.to eq account) }
//                    .sortedBy(XdTransfer::createdAt, asc = false)
//                    .drop(offset)
//                    .take(pageSize)
//                    .toList()
//                    .map { AccountOperation(
//                        dateTime = it.createdAt,
//                        amount = if (it.from == account) it.amount.negate() else it.amount,
//                        currencyCode = it.currencyCode,
//                        contrAccNumber = if (it.from == account) it.to.accNumber else it.from.accNumber
//                    ) }
//
//            }
//
//            Page(content = content, page = page, pageSize = pageSize, totalCount = totalCount, totalPages = totalPages)
//        }
//}
