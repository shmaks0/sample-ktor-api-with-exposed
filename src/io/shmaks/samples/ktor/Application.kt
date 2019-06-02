package io.shmaks.samples.ktor

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.features.*
import io.ktor.routing.*
import io.ktor.http.*
import de.nielsfalk.ktor.swagger.version.shared.Group
import io.ktor.gson.gson
import io.ktor.http.HttpStatusCode.Companion.Created
import io.ktor.locations.KtorExperimentalLocationsAPI
import io.ktor.locations.Location
import io.ktor.locations.Locations
import io.ktor.server.engine.ApplicationEngine
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.shmaks.samples.ktor.model.initExposed
import io.shmaks.samples.ktor.service.*
import org.koin.dsl.module
import org.koin.ktor.ext.Koin
import org.koin.ktor.ext.inject
import java.math.BigDecimal

/*@KtorExperimentalLocationsAPI
@Group("accounts operations")
@Location("/api/v1/accounts")
class accounts

@KtorExperimentalLocationsAPI
@Group("transfers operations")
@Location("/api/v1/transfers")
class transfers

data class Model<T>(val elements: MutableList<T>)

val sizeSchemaMap = mapOf(
    "type" to "number",
    "minimum" to 0
)

fun rectangleSchemaMap(refBase: String) = mapOf(
    "type" to "object",
    "properties" to mapOf(
        "a" to mapOf("${'$'}ref" to "$refBase/size"),
        "b" to mapOf("${'$'}ref" to "$refBase/size")
    )
)*/

data class NewAccountModel(val name: String, val clientId: String, val currencyCode: String, val balance: BigDecimal)
data class NewAccountResponse(val accNumber: Long)
data class TransferModel(val from: Long, val to: Long, val currencyCode: String, val amount: BigDecimal)

@KtorExperimentalLocationsAPI
internal fun run(port: Int, wait: Boolean = true): ApplicationEngine {
    println("Launching on port `$port`")
    val server = embeddedServer(Netty, port) {
        install(CORS) {
            method(HttpMethod.Options)
            method(HttpMethod.Put)
            method(HttpMethod.Delete)
            method(HttpMethod.Patch)
            header(HttpHeaders.Authorization)
            header("MyCustomHeader")
            allowCredentials = true
            anyHost() // @TODO: Don't do this in production if possible. Try to limit it.
        }
        install(Koin) {
            val transaferAppModule = module {
                single<TransferService> { TransferServiceImpl2(get(), get()) }
                single<CurrencyService> { CurrencyServiceImpl() }
                single { initExposed() }
            }
            modules(transaferAppModule)
        }

        install(DefaultHeaders)
        install(Compression)
        install(CallLogging)
        install(ContentNegotiation) {
            gson {
                setPrettyPrinting()
            }
        }
        install(Locations)
//        install(SwaggerSupport) {
//            forwardRoot = true
//            val information = Information(
//                version = "0.1",
//                title = "sample api implemented in ktor"
//            )
//            swagger = Swagger().apply {
//                info = information
//                definitions["size"] = sizeSchemaMap
//                definitions["Rectangle"] = rectangleSchemaMap("#/definitions")
//            }
//            openApi = OpenApi().apply {
//                info = information
//                components.schemas["size"] = sizeSchemaMap
//                components.schemas["Rectangle"] = rectangleSchemaMap("#/components/schemas")
//            }
//        }

        val service: TransferService by inject()

        routing {
//            post<accounts, NewAccountModel>(
//                "create"
//                    .description("Create new account")
//                    .responds(created<NewAccountResponse>())
//            ) { _, entity ->
            post<NewAccountModel>("/api/v1/accounts") { entity ->
                call.respond(Created, NewAccountResponse(
                    service.createAccount(entity.clientId, entity.name, entity.currencyCode, entity.balance)
                ))
            }

//            post<transfers, TransferModel>(
//                "create"
//                    .description("Transfer money")
//                    .responds(HttpCodeResponse(statusCode = HttpStatusCode.OK, responseTypes = emptyList()))
//            ) { _, entity ->
            post<TransferModel>("/api/v1/transfers") { entity ->
                call.respond(HttpStatusCode.OK, service.transferMoney(entity.from, entity.to, entity.amount, entity.currencyCode))
            }

        }
    }
    return server.start(wait = wait)
}

class Launcher : CliktCommand(
    name = "ktor-sample-swagger"
) {
    companion object {
        private const val defaultPort = 8080
    }

    private val port: Int by option(
        "-p",
        "--port",
        help = "The port that this server should be started on. Defaults to $defaultPort."
    )
        .int()
        .default(defaultPort)

    @KtorExperimentalLocationsAPI
    override fun run() {
        run(port)
    }
}

fun main(args: Array<String>) = Launcher().main(args)
