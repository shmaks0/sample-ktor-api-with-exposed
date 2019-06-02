package io.shmaks.samples.io.shmaks.samples.ktor.service

import io.shmaks.samples.ktor.model.XdAccount
import io.shmaks.samples.ktor.model.XdTransfer
import io.shmaks.samples.ktor.model.clearXodus
import io.shmaks.samples.ktor.model.initXodus
import io.shmaks.samples.ktor.service.CurrencyPair
import io.shmaks.samples.ktor.service.CurrencyService
import io.shmaks.samples.ktor.service.TransferService
import io.shmaks.samples.ktor.service.TransferServiceImpl
import jetbrains.exodus.database.TransientEntityStore
import kotlinx.dnq.query.filter
import kotlinx.dnq.query.first
import kotlinx.dnq.query.size
import kotlinx.dnq.query.toList
import org.junit.Assert.fail
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import java.lang.IllegalStateException
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TransferServiceTest {

    private lateinit var service: TransferService

    companion object {
        val dbName = "moneyTransfers-test"
        lateinit var xodusStore: TransientEntityStore
        val currencyService = CurrencyServiceMock()

        @BeforeClass
        @JvmStatic fun setup() {
            clearXodus(dbName)
            xodusStore = initXodus(dbName)
        }
    }

    @Before
    fun setUp() {
        service = TransferServiceImpl(xodusStore, currencyService)
    }

    @Test(expected = IllegalArgumentException::class)
    fun createAccountWithInvalidCurrency() {
       service.createAccount("123", "123-rub", "RUB")
    }

    @Test
    fun createValidAccount() {
        val accountName = "0123-CUR1"
        val accNumber = service.createAccount("0123", accountName, "CUR1")

        xodusStore.transactional(readonly = true) {
            val found = XdAccount.filter { acc -> acc.accNumber eq accNumber }.first()
            assertEquals(accountName, found.name)
            assertEquals(BigDecimal.ZERO, found.balance)
        }
    }

    @Test
    fun createValidAccountWithNonZeroBalance() {
        val accountName = "0123-CUR1"
        val initialBalance = BigDecimal.valueOf(1000.50)
        val accNumber = service.createAccount("0123", accountName, "CUR1", initialBalance)

        xodusStore.transactional(readonly = true) {
            val found = XdAccount.filter { acc -> acc.accNumber eq accNumber }.first()
            assertEquals(accountName, found.name)
            assertEquals(initialBalance, found.balance)
        }
    }

    @Test
    fun makeTransferInOneCurrency() {
        val fromAccNumber = service.createAccount("0123", "0123-CUR1", "CUR1", BigDecimal.valueOf(1000))
        val toAccNumber = service.createAccount("0456", "0456-CUR1", "CUR1")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(1000), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(0), toAcc.balance)

            assertEquals(0, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
        }

        service.transferMoney(fromAccNumber, toAccNumber, BigDecimal.valueOf(300), "CUR1")

        xodusStore.transactional(readonly = true) {
            val fromAcc = XdAccount.filter { acc -> acc.accNumber eq fromAccNumber }.first()
            val toAcc = XdAccount.filter { acc -> acc.accNumber eq toAccNumber }.first()
            assertEquals(BigDecimal.valueOf(700), fromAcc.balance)
            assertEquals(BigDecimal.valueOf(300), toAcc.balance)

            assertEquals(1, XdTransfer.filter{ (it.from eq fromAcc) or (it.to eq toAcc) }.size())
            val transfer = XdTransfer.all().toList()[0]
            assertEquals(fromAcc, transfer.from)
            assertEquals(toAcc, transfer.to)
            assertEquals(BigDecimal.valueOf(300), transfer.amount)
        }
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
