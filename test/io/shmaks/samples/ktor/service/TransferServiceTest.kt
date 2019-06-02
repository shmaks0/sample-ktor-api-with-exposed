package io.shmaks.samples.io.shmaks.samples.ktor.service

import io.shmaks.samples.ktor.model.*
import io.shmaks.samples.ktor.service.*
import kotlinx.coroutines.*
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
//        lateinit var xodusStore: TransientEntityStore
        val currencyService = CurrencyServiceMock()

        val dataSource = initExposed()

        @BeforeClass
        @JvmStatic fun setup() {
//            clearXodus(dbName)
//            xodusStore = initXodus(dbName)
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

    }

    @Test
    fun requestTransferWithLargerAmountInOneCurrency() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1", BigDecimal.valueOf(300))

        Database.connect(dataSource)

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(100).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(300).compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

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

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(100).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(300).compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

        }

    }

    @Test
    fun makeTransferInMultipleCurrencies() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR3", "CUR3")

        Database.connect(dataSource)

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(100).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(0).compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

        }

        service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(100), "CUR2")

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(50).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(200).compareTo(toAcc[Accounts.balance]) == 0)

            val query =
                (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id])
            assertEquals(1, Transfers.select(query).count())
            val transfer = Transfers.select(query).toList()[0]
            assertEquals(fromAcc[Accounts.id], transfer[Transfers.from])
            assertEquals(toAcc[Accounts.id], transfer[Transfers.to])
            assertTrue(BigDecimal.valueOf(100).compareTo(transfer[Transfers.amount]) == 0)

        }
    }

    @Test
    fun requestTransferWithLargerAmountInMultipleCurrencies() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR2", "CUR2", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1")

        Database.connect(dataSource)

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(100).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(0).compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

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

        Database.connect(dataSource)

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(100).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(0).compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

        }

        service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(199), "CUR3")

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(0).compareTo(fromAcc[Accounts.balance]) < 0)
            assertTrue(BigDecimal.valueOf(1).compareTo(fromAcc[Accounts.balance]) > 0)
            assertTrue(BigDecimal.valueOf(49).compareTo(toAcc[Accounts.balance]) < 0)
            assertTrue(BigDecimal.valueOf(50).compareTo(toAcc[Accounts.balance]) > 0)

            val query =
                (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id])
            assertEquals(1, Transfers.select(query).count())
            val transfer = Transfers.select(query).toList()[0]
            assertEquals(fromAcc[Accounts.id], transfer[Transfers.from])
            assertEquals(toAcc[Accounts.id], transfer[Transfers.to])
            assertTrue(BigDecimal.valueOf(199).compareTo(transfer[Transfers.amount]) == 0)

        }
    }

    @Test
    fun makeMultipleTransfersInOneCurrencyConcurrently() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(100))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1")

        Database.connect(dataSource)

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            assertTrue(BigDecimal.valueOf(100).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(0).compareTo(toAcc[Accounts.balance]) == 0)

            assertEquals(0, Transfers.select{ (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id]) }.count())

        }

        val amountAttempts = arrayOf(50, 60, 70, 80)

        val succeedAmount = AtomicInteger()
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
                            succeedAmount.set(amount)
                        } catch (e: Exception) {
                            failedCount.incrementAndGet()
                        }
                    }
                }
            }
        }

        assertEquals(amountAttempts.size - 1, failedCount.get())

        transaction {

            val fromAcc = Accounts.select { Accounts.accNumber eq fromAccNumber }.first()
            val toAcc = Accounts.select { Accounts.accNumber eq toAccNumber }.first()
            val amount = succeedAmount.get().toDouble()
            assertTrue(BigDecimal.valueOf(100- amount).compareTo(fromAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(amount).compareTo(toAcc[Accounts.balance]) == 0)
            assertTrue(BigDecimal.valueOf(100).compareTo(toAcc[Accounts.balance] + fromAcc[Accounts.balance]) == 0)

            val query =
                (Transfers.from eq fromAcc[Accounts.id]) or (Transfers.to eq toAcc[Accounts.id])
            assertEquals(1, Transfers.select(query).count())
            val transfer = Transfers.select(query).toList()[0]
            assertEquals(fromAcc[Accounts.id], transfer[Transfers.from])
            assertEquals(toAcc[Accounts.id], transfer[Transfers.to])
            assertTrue(BigDecimal.valueOf(amount).compareTo(transfer[Transfers.amount]) == 0)

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
