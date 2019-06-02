package io.shmaks.samples.ktor.model

import jetbrains.exodus.database.TransientEntityStore
import jetbrains.exodus.entitystore.Entity
import kotlinx.dnq.*
import kotlinx.dnq.simple.Constraints
import kotlinx.dnq.simple.DEFAULT_REQUIRED
import kotlinx.dnq.simple.min
import kotlinx.dnq.simple.xdProp
import kotlinx.dnq.store.container.StaticStoreContainer
import kotlinx.dnq.util.XdPropertyCachedProvider
import kotlinx.dnq.util.initMetaData
import java.io.File
import java.math.BigDecimal

class XdAccount(entity: Entity) : XdEntity(entity) {

    companion object : XdNaturalEntityType<XdAccount>()

    var clientId by xdRequiredStringProp()
    var name by xdRequiredStringProp()
    var accNumber by xdRequiredLongProp()
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

fun initXodus(): TransientEntityStore {
    XdModel.registerNodes(
        XdAccount,
        XdTransfer
    )
    val databaseHome = File(System.getProperty("user.home"), "moneyTransfers")
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

fun <R : XdEntity> xdRequiredBigDecimalProp(dbName: String? = null, constraints: Constraints<R, BigDecimal?>? = null) =
    XdPropertyCachedProvider {
        xdProp(
            dbName,
            constraints,
            require = true,
            default = DEFAULT_REQUIRED
        )
    }
