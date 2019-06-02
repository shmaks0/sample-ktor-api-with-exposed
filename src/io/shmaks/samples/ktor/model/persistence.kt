package io.shmaks.samples.ktor.model

import jetbrains.exodus.database.TransientEntityStore
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.env.Environments
import kotlinx.dnq.*
import kotlinx.dnq.simple.*
import kotlinx.dnq.store.container.StaticStoreContainer
import kotlinx.dnq.util.XdPropertyCachedProvider
import kotlinx.dnq.util.initMetaData
import java.io.File
import java.math.BigDecimal

class XdAccount(entity: Entity) : XdEntity(entity) {

    companion object : XdNaturalEntityType<XdAccount>()

    var clientId by xdRequiredStringProp()
    var name by xdRequiredStringProp()
    var accNumber by xdRequiredLongProp(unique = true)
    var createdAt by xdRequiredDateTimeProp()
    var updatedAt by xdRequiredDateTimeProp()

    var currencyCode by xdRequiredStringProp()
    var balance by xdRequiredBigDecimalProp { min(0) }
}

class XdTransfer(entity: Entity) : XdEntity(entity) {

    companion object : XdNaturalEntityType<XdTransfer>()

    var createdAt by xdRequiredDateTimeProp()

    var amount by xdRequiredBigDecimalProp()
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
