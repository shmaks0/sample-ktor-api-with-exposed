package io.shmaks.samples.ktor.model

data class Page<T>(val content: List<T>, val page: Int, val pageSize: Int, val totalPages: Int, val totalCount: Int)
