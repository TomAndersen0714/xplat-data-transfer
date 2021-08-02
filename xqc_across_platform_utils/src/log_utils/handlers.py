#!/usr/bin/python3
# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/07/19

import os, time
from logging.handlers import TimedRotatingFileHandler


class TimedAndSizeRotatingHandler(TimedRotatingFileHandler):
    def __init__(
            self,
            filename,
            when='D',
            interval=1,
            backupCount=7,
            encoding=None,
            delay=False,
            utc=False,
            maxBytes=128 * 1024 * 1024
    ):
        """
        :param filename: the base filename of log file
        :param when: duration of a period
        :param interval: the number of period, and the actual rollover interval
            is when*interval(seconds)
        :param backupCount: the max number of rollover interval of backup log files
        :param maxBytes: the max bytes of a single log file
        """
        TimedRotatingFileHandler.__init__(
            self, filename=filename, when=when, interval=interval,
            backupCount=backupCount, encoding=encoding, delay=delay, utc=utc
        )
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self.fileCount = 0
        self.isSizeRollover = False
        self.isTimeRollover = False

    def computeRollover(self, currentTime):
        """
        Work out a new rollover time based on the specified time.
        """
        # round up to the the integer multiples of the actual interval
        return (int(currentTime) / self.interval + 1) * self.interval

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.
        This method will be invoked whenever a new record need to be output.
        """
        # check the status of file stream
        if self.stream is None:
            self.stream = self._open()

        # check the time rollover condition
        if int(time.time()) >= self.rolloverAt:
            self.isTimeRollover = True
            return 1

        # check the file size rollover condition
        if self.maxBytes > 0:
            msg = "%s\n" % self.format(record)
            # due to non-posix-compliant Windows feature, move the cursor to the end of file
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg) >= self.maxBytes:
                self.isSizeRollover = True
                return 1

        return 0

    def getFilesToDelete(self):
        """
        Determine the files to delete based on the 'backupCount' when rolling over.
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        absPaths = []
        prefix = baseName + "."
        prefixLen = len(prefix)

        # get the expired files
        for fileName in fileNames:
            if fileName[:prefixLen] == prefix:
                # if date suffix of file match the date regex
                dateSuffix = fileName[prefixLen:fileName.rfind('.')]
                if self.extMatch.match(dateSuffix):
                    # format the 'dateSuffix' to local time in seconds and compare it with expiration time
                    fileTimestamp = time.mktime(time.strptime(dateSuffix, self.suffix))
                    expireTimestamp = self.rolloverAt - self.interval * self.backupCount
                    if fileTimestamp < expireTimestamp:
                        absPaths.append(os.path.join(dirName, fileName))
        return absPaths

    def doRollover(self):
        """
        Do a rollover.
        In this case, a date stamp with a count is appended to the filename
        when the rollover happens. If there is a backup count, then will remove
        the files with the earliest date suffix.
        """
        if not self.isTimeRollover and not self.isSizeRollover:
            return

            # close current file stream
        if self.stream:
            self.stream.close()
            self.stream = None

        # get the time that this sequence started at and make it a TimeTuple
        currentRolloverAt = self.rolloverAt - self.interval
        timeTuple = time.localtime(currentRolloverAt)

        # if this is a time rollover, reset the count of file shard
        self.fileCount = 0 if self.isTimeRollover else self.fileCount

        # get next rotation filename
        dfn = self.rotation_filename(
            self.baseFilename + "."
            + time.strftime(self.suffix, timeTuple) + "." + str(self.fileCount)
        )
        if os.path.exists(dfn):
            os.remove(dfn)
        # rename the base file
        self.rotate(self.baseFilename, dfn)
        self.fileCount += 1

        # if the max number of rollover interval is specified, find the earliest interval
        # and delete the corresponding files
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)

        # open a new base file and update next rollover time
        self.stream = self._open()
        currentTime = int(time.time())
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        self.rolloverAt = newRolloverAt

        self.isTimeRollover = False
        self.isSizeRollover = False
