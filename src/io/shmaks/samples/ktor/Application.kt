package io.shmaks.samples.ktor

import com.fasterxml.jackson.annotation.JsonInclude
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.features.*
import io.ktor.routing.*
import io.ktor.http.*
import com.fasterxml.jackson.databind.*
import io.ktor.jackson.*
import io.shmaks.samples.ktor.model.initXodus
import io.shmaks.samples.ktor.service.CurrencyService
import io.shmaks.samples.ktor.service.CurrencyServiceImpl
import io.shmaks.samples.ktor.service.TransferService
import io.shmaks.samples.ktor.service.TransferServiceImpl
import jetbrains.exodus.database.TransientEntityStore
import jetbrains.exodus.database.exceptions.ConstraintsValidationException
import org.koin.dsl.module
import org.koin.ktor.ext.Koin
import org.koin.ktor.ext.inject
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
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

    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
            setSerializationInclusion(JsonInclude.Include.NON_NULL)
        }
    }

    install(Koin) {
        modules(transaferAppModule)
    }

    install(StatusPages) {
        exception<ConstraintsValidationException> { cause ->
            call.respond(HttpStatusCode.Conflict)
            throw cause
        }

        exception<IllegalArgumentException> { cause ->
            call.respond(HttpStatusCode.BadRequest)
            throw cause
        }

        exception<IllegalStateException> { cause ->
            call.respond(HttpStatusCode.BadRequest)
            throw cause
        }
    }

    val service: TransferService by inject()
    val store: TransientEntityStore by inject()

    environment.monitor.subscribe(ApplicationStopped) { store.close() }

    routing {
        get("/") {
            call.respondText("HELLO WORLD!", contentType = ContentType.Text.Plain)
        }

        get("/json/jackson") {
            call.respond(mapOf("hello" to "world"))
        }
    }
}

val transaferAppModule = module {
    single<TransferService> { TransferServiceImpl(get(), get()) }
    single<CurrencyService> { CurrencyServiceImpl() }
    single { initXodus() }
}



