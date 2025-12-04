package org.example

import com.mongodb.client.model.Projections
import com.mongodb.client.MongoCollection
import java.util.InputMismatchException
import com.mongodb.client.model.Filters
import org.bson.json.JsonWriterSettings
import java.io.File
import com.mongodb.client.MongoClients
import org.bson.Document
import org.json.JSONArray
import java.util.Scanner

import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend

import com.mongodb.client.MongoClient
import com.mongodb.client.model.Aggregates
import org.example.coleccionCoches


//const val NOM_SRV = "mongodb://guille:1234guille@100.25.144.241:27017"
//const val NOM_BD = "cars"
//const val NOM_COLECCION = "coches"
//
//data class Coche(
//    val id_coche: Int? = null,
//    val modelo: String,
//    val marca: String,
//    val consumo: Double,
//    val hp: Int
//)


lateinit var servidor: MongoServer
lateinit var cliente: MongoClient
lateinit var uri: String
lateinit var coleccionCoches: MongoCollection<Document>

const val NOM_BD = "cars"
const val NOM_COLECCION = "coches"

fun conectarBD() {
    servidor = MongoServer(MemoryBackend())
    val address = servidor.bind()
    uri = "mongodb://${address.hostName}:${address.port}"

    cliente = MongoClients.create(uri)
    coleccionCoches = cliente.getDatabase(NOM_BD).getCollection(NOM_COLECCION)

    println("Servidor MongoDB en memoria iniciado en $uri")
}

fun desconectarBD() {
    cliente.close()
    servidor.shutdown()
    println("Servidor MongoDB en memoria finalizado")
}

fun main() {
    conectarBD()
    importarBD("src/main/resources/cars.json", coleccionCoches)

    menu()

    exportarBD(coleccionCoches,"src/main/resources/cars2.json")
    desconectarBD()
}


fun mostrarCoches() {
    println();
    println("**** Listado de coches:")
    coleccionCoches.find().forEach { doc ->
//        val id_coche = doc.getInteger("id_coche")
//        val modelo = doc.getString("modelo")
//        val marca = doc.getString("marca")
//        val consumo = doc.get("consumo").toString().toDouble()
//        val hp = doc.getInteger("hp")

        println("[${doc.getInteger("id_coche")}] " +
                    "modelo: ${doc.getString("modelo")} " +
                    "marca: ${doc.getString("marca")} " +
                    "consumo: ${doc.get("consumo").toString().toDouble()} " +
                    "hp: ${doc.getInteger("hp")} ")
    }
}
fun insertarCoche() {
    //conectar con la BD

    val coleccion = coleccionCoches

    print("ID del coche: ")
    val id_coche = isInt()
    print("Nombre modelo: ")
    val modelo = isString()
    print("Nombre marca: ")
    val marca = isString()
    print("Consumo: ")
    val consumo = isDouble()
    print("Potencia: ")
    val hp = isInt()



    val doc = Document("id_coche", id_coche)
        .append("modelo", modelo)
        .append("marca", marca)
        .append("consumo", consumo)
        .append("hp", hp)


    coleccion.insertOne(doc)
    println("Coche insertado con ID: ${doc.getObjectId("_id")}")

    println("Conexión cerrada")
}


fun menu() {
    var itera = true
    do {
        println()
        println("   Selecciona una opcion: ")
        println("1. Mostrar Coches")
        println("2. Insertar Coche")
        println("3. Eliminar Coche")
        println("4. Actualizar Coche")
        println("5. Varias operaciones")
        println("6. Salir")
        try {
            val select: Int = isInt()
            when (select) {
                1 -> {
                    mostrarCoches()
                }
                2 -> {
                    insertarCoche()
                }
                3 -> {
                    eliminarCoche()
                }
                4 -> {
                    actualizarCoche()

                }
                5 -> {
                    variasOperaciones()
                }
                6 -> {
                    itera = false
                }

                else -> {
                    println("Opcion no valida. Por favor, selecciona una opcion del 1 al 6.")
                }
            }

        } catch (e: InputMismatchException) {
            println("Error: Debes introducir un numero valido.")
        } catch (e: Exception) {
            println("Error: ${e.message}")
        }
    } while (itera)
}

fun isInt():Int{
    while (true){
        val entrada=readln().toIntOrNull()
        if (entrada==null){
            println("Dame un número valido")
        }else{
            return entrada
        }
    }
}
fun isDouble(): Double{
    while (true){
        val entrada=readln().toDoubleOrNull()
        if (entrada==null){
            println("Dame un número valido(Double)")
        }else{
            return entrada
        }
    }
}
fun isString(): String{
    while (true){
        val entrada=readln()
        if (entrada.isBlank()){
            println("Dame un string valido")
        }else{
            return entrada
        }
    }
}


fun importarBD(rutaJSON: String, coleccion: MongoCollection<Document>) {
    println("Iniciando importación de datos desde JSON...")

    val jsonFile = File(rutaJSON)
    if (!jsonFile.exists()) {
        println("No se encontró el archivo JSON a importar")
        return
    }

    // Leer JSON del archivo
    val jsonText = try {
        jsonFile.readText()
    } catch (e: Exception) {
        println("Error leyendo el archivo JSON: ${e.message}")
        return
    }

    val array = try {
        JSONArray(jsonText)
    } catch (e: Exception) {
        println("Error al parsear JSON: ${e.message}")
        return
    }

    // Convertir JSON a Document y eliminar _id si existe
    val documentos = mutableListOf<Document>()
    for (i in 0 until array.length()) {
        val doc = Document.parse(array.getJSONObject(i).toString())
        doc.remove("_id")  // <-- eliminar _id para que MongoDB genere uno nuevo
        documentos.add(doc)
    }

    if (documentos.isEmpty()) {
        println("El archivo JSON está vacío")
        return
    }

    val db = cliente.getDatabase(NOM_BD)

    val nombreColeccion =coleccion.namespace.collectionName

    // Borrar colección si existe
    if (db.listCollectionNames().contains(nombreColeccion)) {
        db.getCollection(nombreColeccion).drop()
        println("Colección '$nombreColeccion' eliminada antes de importar.")
    }

    // Insertar documentos
    try {
        coleccion.insertMany(documentos)
        println("Importación completada: ${documentos.size} documentos de $nombreColeccion.")
    } catch (e: Exception) {
        println("Error importando documentos: ${e.message}")
    }
}



fun exportarBD(coleccion: MongoCollection<Document>, rutaJSON: String) {
    val settings = JsonWriterSettings.builder().indent(true).build()
    val file = File(rutaJSON)
    file.printWriter().use { out ->
        out.println("[")
        val cursor = coleccion.find().iterator()
        var first = true
        while (cursor.hasNext()) {
            if (!first) out.println(",")
            val doc = cursor.next()
            out.print(doc.toJson(settings))
            first = false
        }
        out.println("]")
        cursor.close()
    }

    println("Exportación de ${coleccion.namespace.collectionName} completada")
}








//
//
fun actualizarCoche() {
    //conectar con la BD

    val coleccion = coleccionCoches

    print("ID del coche a modificar: ")
    val id_coche = isInt()


    val coche = coleccion.find(Filters.eq("id_coche", id_coche)).firstOrNull()
    if (coche == null) {
        println("No se encontró ningun coche con id_coche = \"$id_coche\".")
    }
    else {
        println("Coche encontrado( " +
                "modelo: ${coche.getString("modelo")} " +
                "marca: ${coche.getString("marca")} " +
                "consumo: ${coche.get("consumo").toString().toDouble()} " +
                "hp: ${coche.getInteger("hp")} )")

        print("Nombre modelo a modificar: ")
        val modelo = isString()
        print("Nombre marca a modificar: ")
        val marca = isString()
        print("Consumo a modificar: ")
        val consumo = isDouble()
        print("Potencia a modificar: ")
        val hp = isInt()

        // Actualizar el documento
        val result = coleccion.updateMany(
            Filters.eq("id_coche", id_coche),
            Document("\$set",
                Document()
                    .append("modelo", modelo)
                    .append("marca", marca)
                    .append("consumo", consumo)
                    .append("hp", hp)
            ),

            )


        if (result.modifiedCount > 0)
            println("Cosas actualizadas correctamente (${result.modifiedCount} documento modificado).")
        else
            println("No se modificó ningún documento (help).")
    }

    println("Conexión cerrada.")
}
//
//
fun eliminarCoche() {
    //conectar con la BD

    val coleccion = coleccionCoches

    print("ID del coche a eliminar: ")
    val id_coche = isInt()

    val result = coleccion.deleteOne(Filters.eq("id_coche", id_coche))
    if (result.deletedCount > 0)
        println("Coche eliminado correctamente.")
    else
        println("No se encontró ninguna coche con ese ID.")

    println("Conexión cerrada.")
}


fun variasOperaciones() {
    val col = coleccionCoches

    println("*****Coches que que tienen más de 200 de potencia")
    // 1) Filtro: altura > 100
    col.find(Filters.gt("hp", 200)).forEach { println(it.toJson()) }

    println("*****Marca y nombre común de todos los coches")
    // 2) Proyección: solo nombre_comun
    col.find().projection(Projections.include("marca","id_coche")).forEach { println(it.toJson()) }

    println("*****Potencia media de todos los coches")
    // 3) Agregación: media de altura
    val pipeline = listOf(
        Document("\$group", Document("_id", null)
            .append("potenciaMedia", Document("\$avg", "\$hp"))
//            .append("\$sort", Document("potenciaMedia", -1))
        )
    )
    val aggCursor = col.aggregate(pipeline).iterator()
    aggCursor.use {
        while (it.hasNext()) println(it.next().toJson())
    }

}
