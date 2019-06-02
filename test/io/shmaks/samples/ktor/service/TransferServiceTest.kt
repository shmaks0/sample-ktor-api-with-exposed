package io.shmaks.samples.io.shmaks.samples.ktor.service

import com.google.common.util.concurrent.AtomicDouble
import io.shmaks.samples.ktor.model.*
import io.shmaks.samples.ktor.service.*
import jetbrains.exodus.database.TransientEntityStore
import kotlinx.coroutines.*
import kotlinx.dnq.query.*
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.or
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.Assert.fail
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import java.lang.IllegalStateException
import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TransferServiceTest {

    private lateinit var service: TransferService

    companion object {
        val dbName = "moneyTransfers-test"
        lateinit var xodusStore: TransientEntityStore
        val currencyService = CurrencyServiceMock()

        val dataSource = initExposed()

        @BeforeClass
        @JvmStatic fun setup() {
            clearXodus(dbName)
            xodusStore = initXodus(dbName)
        }
    }

    @Before
    fun setUp() {
        service = TransferServiceImpl2(dataSource, currencyService)
    }

    @Test(expected = IllegalArgumentException::class)
    fun createAccountWithInvalidCurrency() {
       service.createAccount("123", "123-rub", "RUB")
    }

    @Test
    fun createValidAccount() {
        val accountName = "0123-CUR1"
        val accNumber = service.createAccount("0123", accountName, "CUR1")

        Database.connect(dataSource)

        transaction {
            val found = Accounts.select { Accounts.accNumber eq accNumber }.first()
            assertEquals(accountName, found[Accounts.name])
            assertTrue(BigDecimal.ZERO.compareTo(found[Accounts.balance]) == 0)
        }

        /*xodusStore.transactional(readonly = true) {
            val found = XdAccount.filter { acc -> acc.accNumber eq accNumber }.first()
            assertEquals(accountName, found.name)
            assertEquals(BigDecimal.ZERO, found.balance)
        }*/
    }

    @Test
    fun createValidAccountWithNonZeroBalance() {
        val accountName = "0123-CUR1"
        val initialBalance = BigDecimal.valueOf(1000.50)
        val accNumber = service.createAccount("0123", accountName, "CUR1", initialBalance)

        Database.connect(dataSource)

        transaction {
            val found = Accounts.select { Accounts.accNumber eq accNumber }.first()
            assertEquals(accountName, found[Accounts.name])
            assertTrue(initialBalance.compareTo(found[Accounts.balance]) == 0)
        }

        /*xodusStore.transactional(readonly = true) {
            val found = XdAccount.filter { acc -> acc.accNumber eq accNumber }.first()
            assertEquals(accountName, found.name)
            assertEquals(initialBalance, found.balance)
        }*/
    }

    @Test
    fun makeTransferInOneCurrency() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(1000))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1")

        Database.connect(dataSource)

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(1000).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.ZERO.compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

        }

//        xodusStore.transactional(readonly = true) {
//            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
//            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
//            assertEquals(BigDecimal.valueOf(1000), fromAcc.balance)
//            assertEquals(BigDecimal.valueOf(0), toAcc.balance)
//
//            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
//        }

        service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(300), "CUR1")

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(700).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(300).compareTo(toAcc[Accounts.balance]) == 0)

            val query =
                (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id])
            assertEquals(1, Transfers.select(query).count())
            val transfer = Transfers.select(query).toList()[0]
            assertEquals(fromAcc[Accounts.id], transfer[Transfers.from])
            assertEquals(toAcc[Accounts.id], transfer[Transfers.to])
            assertTrue(BigDecimal.valueOf(300).compareTo(transfer[Transfers.amount]) == 0)

        }

//        xodusStore.transactional(readonly = true) {
//            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
//            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
//            assertEquals(BigDecimal.valueOf(700), fromAcc.balance)
//            assertEquals(BigDecimal.valueOf(300), toAcc.balance)
//
//            assertEquals(1, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
//            val transfer = XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.toList()[0]
//            assertEquals(fromAcc, transfer.from)
//            assertEquals(toAcc, transfer.to)
//            assertEquals(BigDecimal.valueOf(300), transfer.amount)
//        }
    }

    @Test
    fun requestTransferWithLargerAmountInOneCurrency() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1", BigDecimal.valueOf(300))

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(300), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }

        var exception : Exception? = null
        try {
            service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(200), "CUR1")
            fail("Insufficient funds expected")
        } catch (e: Exception) {
            exception = e
        }

        assertNotNull(exception)
        assertTrue(exception is IllegalStateException)
        assertEquals("Insufficient funds", exception.message)
        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(300), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }
    }

    @Test
    fun makeTransferInMultipleCurrencies() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR3", "CUR3")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(0), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }

        service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(100), "CUR2")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(50).compareTo(fromAcc.balance) == 0)
            assertTrue(BigDecimal.valueOf(200).compareTo(toAcc.balance) == 0)

            assertEquals(1, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
            val transfer = XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.toList()[0]
            assertEquals(fromAcc, transfer.from)
            assertEquals(toAcc, transfer.to)
            assertEquals(BigDecimal.valueOf(100), transfer.amount)
        }
    }

    @Test
    fun requestTransferWithLargerAmountInMultipleCurrencies() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR2", "CUR2", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(0), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }

        var exception : Exception? = null
        try {
            service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(201), "CUR3")
            fail("Insufficient funds expected")
        } catch (e: Exception) {
            exception = e
        }

        assertNotNull(exception)
        assertTrue(exception is IllegalStateException)
        assertEquals("Insufficient funds", exception.message)

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(0), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }

        service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(199), "CUR3")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(0).compareTo(fromAcc.balance) < 0)
            assertTrue(BigDecimal.valueOf(1).compareTo(fromAcc.balance) > 0)
            assertTrue(BigDecimal.valueOf(49).compareTo(toAcc.balance) < 0)
            assertTrue(BigDecimal.valueOf(50).compareTo(toAcc.balance) > 0)

            assertEquals(1, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
            val transfer = XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.toList()[0]
            assertEquals(fromAcc.accNumber, transfer.from.accNumber)
            assertEquals(toAcc.accNumber, transfer.to.accNumber)
            assertEquals(BigDecimal.valueOf(199), transfer.amount)
        }
    }

    @Test
    fun makeMultipleTransfersInOneCurrencyConcurrently() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(0), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }

        val amountAttempts = arrayOf(50, 60, 70, 80)

        val succeedAmount = AtomicDouble()
        val failedCount = AtomicInteger()

        runBlocking {
            withContext(Dispatchers.Default) {
                amountAttempts.forEach { amount ->
                    launch {
                        try {
                            service.transferMoney(
                                fromAccNumber,
                                toAccNumber,
                                BigDecimal.valueOf(amount.toDouble()),
                                "CUR1"
                            )
                            succeedAmount.set(amount.toDouble())
                        } catch (e: Exception) {
                            failedCount.incrementAndGet()
                        }
                    }
                }
            }
        }

//        assertEquals(amountAttempts.size - 1, failedCount.get())

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(100-succeedAmount.get()), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(succeedAmount.get()), toAcc.balance)
            assertTrue(BigDecimal.valueOf(100).compareTo(toAcc.balance + fromAcc.balance) == 0)

            assertEquals(1, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
            val transfer = XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.toList()[0]
            assertEquals(fromAcc, transfer.from)
            assertEquals(toAcc, transfer.to)
            assertEquals(BigDecimal.valueOf(succeedAmount.get()), transfer.amount)
        }
    }
}


class CurrencyServiceMock : CurrencyService {
    private val rates = mapOf(
        "CUR1" to mapOf(
            "CUR2" to BigDecimal.valueOf(2),
            "CUR3" to BigDecimal.valueOf( 4)
        ),

        "CUR2" to mapOf(
            "CUR1" to BigDecimal.valueOf(0.5),
            "CUR3" to BigDecimal.valueOf( 2)
        ),

        "CUR3" to mapOf(
            "CUR1" to BigDecimal.valueOf(0.25),
            "CUR2" to BigDecimal.valueOf(0.5)
        )
    )

    override fun supportCurrency(code: String) = rates.containsKey(code)

    override fun exchangeRates(currencyPairs: Array<CurrencyPair>): Map<Int, BigDecimal> =
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
