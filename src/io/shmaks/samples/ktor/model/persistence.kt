package io.shmaks.samples.ktor.model

import org.h2.jdbcx.JdbcDataSource
import org.jetbrains.exposed.dao.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction

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

fun initExposed(): JdbcDataSource {
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")

    Database.connect(ds)

    transaction {
        SchemaUtils.create(Accounts, Transfers)
    }

    return ds
}
