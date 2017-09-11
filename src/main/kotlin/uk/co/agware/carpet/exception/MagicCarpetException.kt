package uk.co.agware.carpet.exception;

abstract class MagicCarpetException(message: String) : RuntimeException(message)

class MagicCarpetDatabaseException(message: String) : MagicCarpetException(message)

class MagicCarpetParseException(message: String) : MagicCarpetException(message)
