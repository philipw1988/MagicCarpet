package uk.co.agware.carpet.exception;
/**
 * Created by Simon on 29/12/2016.
 */
// TODO Can get rid of the unused constructors
class MagicCarpetException : RuntimeException {

    constructor(message: String?, cause: Throwable) : super(message, cause){}
    constructor(message: String?) : super(message){}
    constructor(cause: Throwable) : super(cause){}
    constructor(message: String?, cause: Throwable, enableSuppression: Boolean, writableStackTrace: Boolean) : super(message, cause, enableSuppression, writableStackTrace){}

}
