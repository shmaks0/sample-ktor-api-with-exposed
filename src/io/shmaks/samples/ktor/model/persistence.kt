package io.shmaks.samples.ktor.model

import com.jetbrains.teamsys.dnq.database.PropertyConstraint
import jetbrains.exodus.database.TransientEntityStore
import jetbrains.exodus.entitystore.Entity
import kotlinx.dnq.*
import kotlinx.dnq.simple.*
import kotlinx.dnq.store.container.StaticStoreContainer
import kotlinx.dnq.util.XdPropertyCachedProvider
import kotlinx.dnq.util.initMetaData
import org.h2.jdbcx.JdbcDataSource
import org.jetbrains.exposed.dao.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.math.BigDecimal

object Accounts : LongIdTable() {
    val clientId = varchar("client_id", 32)
    val name = varchar("name", 50)
    val accNumber = long("acc_number").uniqueIndex().autoIncrement()
    val createdAt = datetime("created_at")
    val updatedAt = datetime("updated_at")
    val currencyCode = varchar("currency_code", 8)
    val balance = decimal("balance", 20, 4)
}

object Transfers : LongIdTable() {
    val createdAt = datetime("created_at")
    val currencyCode = varchar("currency_code", 8)
    val amount = decimal("amount", 20, 4)

    val from = reference("from_acc_id", Accounts)
    val to = reference("to_acc_id", Accounts)
}

class XdAccount(entity: Entity) : XdEntity(entity) {

    companion object : XdNaturalEntityType<XdAccount>()

    var clientId by xdRequiredStringProp()
    var name by xdRequiredStringProp()
    var accNumber by xdRequiredLongProp(unique = true)
    var createdAt by xdRequiredDateTimeProp()
    var updatedAt by xdRequiredDateTimeProp()

    var currencyCode by xdRequiredStringProp()
    var balance by xdRequiredBigDecimalProp{ nonNegative() }
}

class XdTransfer(entity: Entity) : XdEntity(entity) {

    companion object : XdNaturalEntityType<XdTransfer>()

    var createdAt by xdRequiredDateTimeProp()

    var amount by xdRequiredBigDecimalProp{ nonNegative() }
    var currencyCode by xdRequiredStringProp()

    var from by xdLink1(XdAccount)
    var to by xdLink1(XdAccount)
}

fun initXodus(dbName: String = "moneyTransfers"): TransientEntityStore {
    XdModel.registerNodes(
        XdAccount,
        XdTransfer
    )
    val databaseHome = File(System.getProperty("user.home"), dbName)
    val store = StaticStoreContainer.init(
        dbFolder = databaseHome,
        environmentName = "db"
    )
    initMetaData(XdModel.hierarchy, store)

    store.transactional {
       it.getSequence("accountNumbers").set(1024)
    }

    return store
}

fun initExposed(): JdbcDataSource {
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")

    Database.connect(ds)

    transaction {
        SchemaUtils.create(Accounts, Transfers)
    }

    return ds
}

fun clearXodus(dbName: String = "moneyTransfers") {
    File(System.getProperty("user.home"), dbName).deleteRecursively()
}

fun <R : XdEntity> xdRequiredBigDecimalProp(dbName: String? = null, constraints: Constraints<R, BigDecimal?>? = null) =
    XdPropertyCachedProvider {
        XdProperty<R, String>(
            String::class.java,
            dbName,
            constraints.collect().wrap<R, String, BigDecimal> { BigDecimal(it) },
            requirement = XdPropertyRequirement.REQUIRED,
            default = DEFAULT_REQUIRED
        ).wrap({ BigDecimal(it) }, { it.toPlainString() })
    }

fun PropertyConstraintBuilder<*, BigDecimal?>.nonNegative() {
    constraints.add(object : PropertyConstraint<BigDecimal?>() {

        override fun isValid(value: BigDecimal?) = BigDecimal.ZERO.compareTo(value) <= 0

        override fun getExceptionMessage(propertyName: String, propertyValue: BigDecimal?): String {
            if (BigDecimal.ZERO.compareTo(propertyValue) <= 0) {
                return ""
            }

            return "$propertyName should be non-negative value but was $propertyValue"
        }
    })
}
