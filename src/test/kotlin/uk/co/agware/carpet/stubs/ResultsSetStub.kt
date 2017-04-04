package uk.co.agware.carpet.stubs

import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*


/**
 * Created by simon on 29/01/2017.
 */
class ResultsSetStub(val hasNext: Boolean = true, val hash: String? = null) : ResultSet {


    @Throws(SQLException::class)
    override fun next(): Boolean {
        return hasNext
    }

    @Throws(SQLException::class)
    override fun close() {

    }

    @Throws(SQLException::class)
    override fun wasNull(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getString(columnIndex: Int): String? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnIndex: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getByte(columnIndex: Int): Byte {
        return 0
    }

    @Throws(SQLException::class)
    override fun getShort(columnIndex: Int): Short {
        return 0
    }

    @Throws(SQLException::class)
    override fun getInt(columnIndex: Int): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun getLong(columnIndex: Int): Long {
        return 0
    }

    @Throws(SQLException::class)
    override fun getFloat(columnIndex: Int): Float {
        return 0f
    }

    @Throws(SQLException::class)
    override fun getDouble(columnIndex: Int): Double {
        return 0.0
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBytes(columnIndex: Int): ByteArray {
        return ByteArray(0)
    }

    @Throws(SQLException::class)
    override fun getDate(columnIndex: Int): Date? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTime(columnIndex: Int): Time? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnIndex: Int): Timestamp? {
        return null
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(columnIndex: Int): InputStream? {
        return null
    }

    @Throws(SQLException::class)
    override fun getUnicodeStream(columnIndex: Int): InputStream? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBinaryStream(columnIndex: Int): InputStream? {
        return null
    }

    @Throws(SQLException::class)
    override fun getString(columnLabel: String): String? {
        return hash
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnLabel: String): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getByte(columnLabel: String): Byte {
        return 0
    }

    @Throws(SQLException::class)
    override fun getShort(columnLabel: String): Short {
        return 0
    }

    @Throws(SQLException::class)
    override fun getInt(columnLabel: String): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun getLong(columnLabel: String): Long {
        return 0
    }

    @Throws(SQLException::class)
    override fun getFloat(columnLabel: String): Float {
        return 0f
    }

    @Throws(SQLException::class)
    override fun getDouble(columnLabel: String): Double {
        return 0.0
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBytes(columnLabel: String): ByteArray {
        return ByteArray(0)
    }

    @Throws(SQLException::class)
    override fun getDate(columnLabel: String): Date? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTime(columnLabel: String): Time? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnLabel: String): Timestamp? {
        return null
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(columnLabel: String): InputStream? {
        return null
    }

    @Throws(SQLException::class)
    override fun getUnicodeStream(columnLabel: String): InputStream? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBinaryStream(columnLabel: String): InputStream? {
        return null
    }

    @Throws(SQLException::class)
    override fun getWarnings(): SQLWarning? {
        return null
    }

    @Throws(SQLException::class)
    override fun clearWarnings() {

    }

    @Throws(SQLException::class)
    override fun getCursorName(): String? {
        return null
    }

    @Throws(SQLException::class)
    override fun getMetaData(): ResultSetMetaData? {
        return null
    }

    @Throws(SQLException::class)
    override fun getObject(columnIndex: Int): Any? {
        return null
    }

    @Throws(SQLException::class)
    override fun getObject(columnLabel: String): Any? {
        return null
    }

    @Throws(SQLException::class)
    override fun findColumn(columnLabel: String): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(columnIndex: Int): Reader? {
        return null
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(columnLabel: String): Reader? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnLabel: String): BigDecimal? {
        return null
    }

    @Throws(SQLException::class)
    override fun isBeforeFirst(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun isAfterLast(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun isFirst(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun isLast(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun beforeFirst() {

    }

    @Throws(SQLException::class)
    override fun afterLast() {

    }

    @Throws(SQLException::class)
    override fun first(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun last(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getRow(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun absolute(row: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun relative(rows: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun previous(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun setFetchDirection(direction: Int) {

    }

    @Throws(SQLException::class)
    override fun getFetchDirection(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun setFetchSize(rows: Int) {

    }

    @Throws(SQLException::class)
    override fun getFetchSize(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun getType(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun getConcurrency(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun rowUpdated(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun rowInserted(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun rowDeleted(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun updateNull(columnIndex: Int) {

    }

    @Throws(SQLException::class)
    override fun updateBoolean(columnIndex: Int, x: Boolean) {

    }

    @Throws(SQLException::class)
    override fun updateByte(columnIndex: Int, x: Byte) {

    }

    @Throws(SQLException::class)
    override fun updateShort(columnIndex: Int, x: Short) {

    }

    @Throws(SQLException::class)
    override fun updateInt(columnIndex: Int, x: Int) {

    }

    @Throws(SQLException::class)
    override fun updateLong(columnIndex: Int, x: Long) {

    }

    @Throws(SQLException::class)
    override fun updateFloat(columnIndex: Int, x: Float) {

    }

    @Throws(SQLException::class)
    override fun updateDouble(columnIndex: Int, x: Double) {

    }

    @Throws(SQLException::class)
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal) {

    }

    @Throws(SQLException::class)
    override fun updateString(columnIndex: Int, x: String) {

    }

    @Throws(SQLException::class)
    override fun updateBytes(columnIndex: Int, x: ByteArray) {

    }

    @Throws(SQLException::class)
    override fun updateDate(columnIndex: Int, x: Date) {

    }

    @Throws(SQLException::class)
    override fun updateTime(columnIndex: Int, x: Time) {

    }

    @Throws(SQLException::class)
    override fun updateTimestamp(columnIndex: Int, x: Timestamp) {

    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnIndex: Int, x: InputStream, length: Int) {

    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnIndex: Int, x: InputStream, length: Int) {

    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnIndex: Int, x: Reader, length: Int) {

    }

    @Throws(SQLException::class)
    override fun updateObject(columnIndex: Int, x: Any, scaleOrLength: Int) {

    }

    @Throws(SQLException::class)
    override fun updateObject(columnIndex: Int, x: Any) {

    }

    @Throws(SQLException::class)
    override fun updateNull(columnLabel: String) {

    }

    @Throws(SQLException::class)
    override fun updateBoolean(columnLabel: String, x: Boolean) {

    }

    @Throws(SQLException::class)
    override fun updateByte(columnLabel: String, x: Byte) {

    }

    @Throws(SQLException::class)
    override fun updateShort(columnLabel: String, x: Short) {

    }

    @Throws(SQLException::class)
    override fun updateInt(columnLabel: String, x: Int) {

    }

    @Throws(SQLException::class)
    override fun updateLong(columnLabel: String, x: Long) {

    }

    @Throws(SQLException::class)
    override fun updateFloat(columnLabel: String, x: Float) {

    }

    @Throws(SQLException::class)
    override fun updateDouble(columnLabel: String, x: Double) {

    }

    @Throws(SQLException::class)
    override fun updateBigDecimal(columnLabel: String, x: BigDecimal) {

    }

    @Throws(SQLException::class)
    override fun updateString(columnLabel: String, x: String) {

    }

    @Throws(SQLException::class)
    override fun updateBytes(columnLabel: String, x: ByteArray) {

    }

    @Throws(SQLException::class)
    override fun updateDate(columnLabel: String, x: Date) {

    }

    @Throws(SQLException::class)
    override fun updateTime(columnLabel: String, x: Time) {

    }

    @Throws(SQLException::class)
    override fun updateTimestamp(columnLabel: String, x: Timestamp) {

    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnLabel: String, x: InputStream, length: Int) {

    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnLabel: String, x: InputStream, length: Int) {

    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnLabel: String, reader: Reader, length: Int) {

    }

    @Throws(SQLException::class)
    override fun updateObject(columnLabel: String, x: Any, scaleOrLength: Int) {

    }

    @Throws(SQLException::class)
    override fun updateObject(columnLabel: String, x: Any) {

    }

    @Throws(SQLException::class)
    override fun insertRow() {

    }

    @Throws(SQLException::class)
    override fun updateRow() {

    }

    @Throws(SQLException::class)
    override fun deleteRow() {

    }

    @Throws(SQLException::class)
    override fun refreshRow() {

    }

    @Throws(SQLException::class)
    override fun cancelRowUpdates() {

    }

    @Throws(SQLException::class)
    override fun moveToInsertRow() {

    }

    @Throws(SQLException::class)
    override fun moveToCurrentRow() {

    }

    @Throws(SQLException::class)
    override fun getStatement(): Statement? {
        return null
    }

    @Throws(SQLException::class)
    override fun getObject(columnIndex: Int, map: Map<String, Class<*>>): Any? {
        return null
    }

    @Throws(SQLException::class)
    override fun getRef(columnIndex: Int): Ref? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBlob(columnIndex: Int): Blob? {
        return null
    }

    @Throws(SQLException::class)
    override fun getClob(columnIndex: Int): Clob? {
        return null
    }

    @Throws(SQLException::class)
    override fun getArray(columnIndex: Int): Array? {
        return null
    }

    @Throws(SQLException::class)
    override fun getObject(columnLabel: String, map: Map<String, Class<*>>): Any? {
        return null
    }

    @Throws(SQLException::class)
    override fun getRef(columnLabel: String): Ref? {
        return null
    }

    @Throws(SQLException::class)
    override fun getBlob(columnLabel: String): Blob? {
        return null
    }

    @Throws(SQLException::class)
    override fun getClob(columnLabel: String): Clob? {
        return null
    }

    @Throws(SQLException::class)
    override fun getArray(columnLabel: String): java.sql.Array? {
        return null
    }

    @Throws(SQLException::class)
    override fun getDate(columnIndex: Int, cal: Calendar): Date? {
        return null
    }

    @Throws(SQLException::class)
    override fun getDate(columnLabel: String, cal: Calendar): Date? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTime(columnIndex: Int, cal: Calendar): Time? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTime(columnLabel: String, cal: Calendar): Time? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnIndex: Int, cal: Calendar): Timestamp? {
        return null
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnLabel: String, cal: Calendar): Timestamp? {
        return null
    }

    @Throws(SQLException::class)
    override fun getURL(columnIndex: Int): URL? {
        return null
    }

    @Throws(SQLException::class)
    override fun getURL(columnLabel: String): URL? {
        return null
    }

    @Throws(SQLException::class)
    override fun updateRef(columnIndex: Int, x: Ref) {

    }

    @Throws(SQLException::class)
    override fun updateRef(columnLabel: String, x: Ref) {

    }

    @Throws(SQLException::class)
    override fun updateBlob(columnIndex: Int, x: Blob) {

    }

    @Throws(SQLException::class)
    override fun updateBlob(columnLabel: String, x: Blob) {

    }

    @Throws(SQLException::class)
    override fun updateClob(columnIndex: Int, x: Clob) {

    }

    @Throws(SQLException::class)
    override fun updateClob(columnLabel: String, x: Clob) {

    }

    @Throws(SQLException::class)
    override fun updateArray(columnIndex: Int, x: Array) {

    }

    @Throws(SQLException::class)
    override fun updateArray(columnLabel: String, x: Array) {

    }

    @Throws(SQLException::class)
    override fun getRowId(columnIndex: Int): RowId? {
        return null
    }

    @Throws(SQLException::class)
    override fun getRowId(columnLabel: String): RowId? {
        return null
    }

    @Throws(SQLException::class)
    override fun updateRowId(columnIndex: Int, x: RowId) {

    }

    @Throws(SQLException::class)
    override fun updateRowId(columnLabel: String, x: RowId) {

    }

    @Throws(SQLException::class)
    override fun getHoldability(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun isClosed(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun updateNString(columnIndex: Int, nString: String) {

    }

    @Throws(SQLException::class)
    override fun updateNString(columnLabel: String, nString: String) {

    }

    @Throws(SQLException::class)
    override fun updateNClob(columnIndex: Int, nClob: NClob) {

    }

    @Throws(SQLException::class)
    override fun updateNClob(columnLabel: String, nClob: NClob) {

    }

    @Throws(SQLException::class)
    override fun getNClob(columnIndex: Int): NClob? {
        return null
    }

    @Throws(SQLException::class)
    override fun getNClob(columnLabel: String): NClob? {
        return null
    }

    @Throws(SQLException::class)
    override fun getSQLXML(columnIndex: Int): SQLXML? {
        return null
    }

    @Throws(SQLException::class)
    override fun getSQLXML(columnLabel: String): SQLXML? {
        return null
    }

    @Throws(SQLException::class)
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML) {

    }

    @Throws(SQLException::class)
    override fun updateSQLXML(columnLabel: String, xmlObject: SQLXML) {

    }

    @Throws(SQLException::class)
    override fun getNString(columnIndex: Int): String? {
        return null
    }

    @Throws(SQLException::class)
    override fun getNString(columnLabel: String): String? {
        return null
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(columnIndex: Int): Reader? {
        return null
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(columnLabel: String): Reader? {
        return null
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnLabel: String, reader: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnIndex: Int, x: InputStream, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnIndex: Int, x: InputStream, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnIndex: Int, x: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnLabel: String, x: InputStream, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnLabel: String, x: InputStream, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnLabel: String, reader: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateBlob(columnIndex: Int, inputStream: InputStream, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateBlob(columnLabel: String, inputStream: InputStream, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateClob(columnIndex: Int, reader: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateClob(columnLabel: String, reader: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateNClob(columnIndex: Int, reader: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateNClob(columnLabel: String, reader: Reader, length: Long) {

    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnIndex: Int, x: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnLabel: String, reader: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnIndex: Int, x: InputStream) {

    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnIndex: Int, x: InputStream) {

    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnIndex: Int, x: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnLabel: String, x: InputStream) {

    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnLabel: String, x: InputStream) {

    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnLabel: String, reader: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateBlob(columnIndex: Int, inputStream: InputStream) {

    }

    @Throws(SQLException::class)
    override fun updateBlob(columnLabel: String, inputStream: InputStream) {

    }

    @Throws(SQLException::class)
    override fun updateClob(columnIndex: Int, reader: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateClob(columnLabel: String, reader: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateNClob(columnIndex: Int, reader: Reader) {

    }

    @Throws(SQLException::class)
    override fun updateNClob(columnLabel: String, reader: Reader) {

    }

    @Throws(SQLException::class)
    override fun <T> getObject(columnIndex: Int, type: Class<T>): T? {
        return null
    }

    @Throws(SQLException::class)
    override fun <T> getObject(columnLabel: String, type: Class<T>): T? {
        return null
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T? {
        return null
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>): Boolean {
        return false
    }
}