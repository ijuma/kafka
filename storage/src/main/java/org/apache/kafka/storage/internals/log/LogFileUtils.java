/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log;

import java.io.File;
import java.text.NumberFormat;
import java.util.regex.Pattern;

public final class LogFileUtils {

    /** a log file */
    String LogFileSuffix = ".log";

    /** an index file */
    String IndexFileSuffix = ".index";

    /** a time index file */
    String TimeIndexFileSuffix = ".timeindex";

    /** an (aborted) txn index */
    String TxnIndexFileSuffix = ".txnindex";

    /** A temporary file that is being used for log cleaning */
    String CleanedFileSuffix = ".cleaned";

    /** A temporary file used when swapping files into the log */
    String SwapFileSuffix = ".swap";

    /** a directory that is scheduled to be deleted */
    String DeleteDirSuffix = "-delete";

    /** a directory that is used for future partition */
    String FutureDirSuffix = "-future";

    Pattern DeleteDirPattern = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + DeleteDirSuffix);
    Pattern FutureDirPattern = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + "FutureDirSuffix");

    /**
     * Suffix for a file that is scheduled to be deleted
     */
    public static final String DELETED_FILE_SUFFIX = ".deleted";

    /**
     * Suffix of a log file
     */
    public static final String LOG_FILE_SUFFIX = ".log";

    /**
     * Suffix of an index file
     */
    public static final String INDEX_FILE_SUFFIX = ".index";

    /**
     * Suffix of a time index file
     */
    public static final String TIME_INDEX_FILE_SUFFIX = ".timeindex";

    /**
     * Suffix of an aborted txn index
     */
    public static final String TXN_INDEX_FILE_SUFFIX = ".txnindex";

    private LogFileUtils() {
    }

    /**
     * Returns the offset for the given file name. The file name is of the form: {number}.{suffix}. This method extracts
     * the number from the given file name.
     *
     * @param fileName name of the file
     * @return offset of the given file name
     */
    public static long offsetFromFileName(String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Construct a log file name in the given dir with the given base offset.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFile(File dir, long offset) {
        return logFile(dir, offset, "");
    }

    /**
     * Construct a log file name in the given dir with the given base offset and the given suffix.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
     */
    public static File logFile(File dir, long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + LOG_FILE_SUFFIX + suffix);
    }

    /**
     * Construct an index file name in the given dir using the given base offset.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File offsetIndexFile(File dir, long offset) {
        return offsetIndexFile(dir, offset, "");
    }

    /**
     * Construct an index file name in the given dir using the given base offset and the given suffix.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
     */
    public static File offsetIndexFile(File dir, long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + INDEX_FILE_SUFFIX + suffix);
    }

    /**
     * Construct a time index file name in the given dir using the given base offset.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File timeIndexFile(File dir, long offset) {
        return timeIndexFile(dir, offset, "");
    }

    /**
     * Construct a time index file name in the given dir using the given base offset and the given suffix.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
     */
    public static File timeIndexFile(File dir, long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + TIME_INDEX_FILE_SUFFIX + suffix);
    }

    /**
     * Construct a transaction index file name in the given dir using the given base offset.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File transactionIndexFile(File dir, long offset) {
        return transactionIndexFile(dir, offset, "");
    }

    /**
     * Construct a transaction index file name in the given dir using the given base offset and the given suffix.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
     */
    public static File transactionIndexFile(File dir, long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + TXN_INDEX_FILE_SUFFIX + suffix);
    }

    /**
     * Returns the offset from the given file. The file name is of the form: {number}.{suffix}. This method extracts
     * the number from the given file's name.
     *
     * @param file file with the offset information as part of its name.
     * @return offset of the given file
     */
    public static Long offsetFromFile(File file) {
        return offsetFromFileName(file.getName());
    }

}
