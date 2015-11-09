from __builtin__ import open, type

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>', 'Marco Strutz <marco.strutz@desy.de>'


import time
import argparse
import zmq
import os
import logging
import sys
import json
import traceback
from multiprocessing import Process, freeze_support
import subprocess
import json
import shutil
import collections


DEFAULT_CHUNK_SIZE = 1048576


#
#  --------------------------  class: Cleaner  --------------------------------------
#
class Cleaner():
    """
    * received cleaning jobs via zeromq,
      such as removing a file
    * Does regular checks on the watched directory,
    such as
      - deleting files which have been successfully send
        to target but still remain in the watched directory
      - poll the watched directory and reissue new files
        to fileMover which have not been detected yet
    """
    bindingPortForSocket = None
    bindingIpForSocket   = None
    senderComIp          = None
    zmqContextForCleaner = None
    externalContext      = None    # if the context was created outside this class or not
    zmqCleanerSocket     = None

    useDataStream        = True      # boolian to inform if the data should be send to the data stream pipe (to the storage system)

    lastMovedFiles       = collections.deque(maxlen = 20)

    # to get the logging only handling this class
    log                  = None

    def __init__(self, targetPath, bindingIp="127.0.0.1", bindingPort="6062", useDataStream = True, context = None, verbose=False):
        self.bindingPortForSocket = bindingPort
        self.bindingIpForSocket   = bindingIp
        self.senderComIp          = self.bindingIpForSocket
        self.targetPath           = os.path.normpath(targetPath)

        self.useDataStream       = useDataStream

        if context:
            self.zmqContextForCleaner = context
            self.externalContext      = True
        else:
            self.zmqContextForCleaner = zmq.Context()
            self.externalContext      = False

        self.log = self.getLogger()
        self.log.debug("Init")

        #bind to local port
        self.zmqCleanerSocket = self.zmqContextForCleaner.socket(zmq.PULL)
        connectionStrSocket   = "tcp://" + self.bindingIpForSocket + ":%s" % self.bindingPortForSocket
        self.zmqCleanerSocket.bind(connectionStrSocket)
        self.log.debug("zmqCleanerSocket started for '" + connectionStrSocket + "'")

        try:
            self.process()
        except KeyboardInterrupt:
            self.log.debug("KeyboardInterrupt detected. Shutting down cleaner.")
        except Exception as e:
            trace = traceback.format_exc()
            self.log.error("Stopping cleanerProcess due to unknown error condition.")
            self.log.debug("Error was: " + str(e))
            self.log.debug("Trace was: " + str(trace))
        finally:
            self.stop()


    def getLogger(self):
        logger = logging.getLogger("cleaner")
        return logger


    def process(self):
        #processing messaging
        while True:
            #waiting for new jobs
            self.log.debug("Waiting for new jobs")

            try:
                workload = self.zmqCleanerSocket.recv()
            except Exception as e:
                self.log.error("Error in receiving job: " + str(e))

            if workload == "STOP":
                self.log.debug("Stopping cleaner")
                break

            # transform to dictionary
            # metadataDict = {
            #   "filename"             : filename,
            #   "filesize"             : filesize,
            #   "fileModificationTime" : fileModificationTime,
            #   "sourcePath"           : sourcePath,
            #   "relativePath"         : relativePath,
            #   "chunkSize"            : self.getChunkSize()
            #   }
            try:
                workloadDict = json.loads(str(workload))
            except:
                errorMessage = "invalid job received. skipping job"
                self.log.error(errorMessage)
                self.log.debug("workload=" + str(workload))
                continue

            #extract fileEvent metadata/data
            try:
                #TODO validate fileEventMessageDict dict
                filename       = workloadDict["filename"]
                sourcePath     = workloadDict["sourcePath"]
                relativePath   = workloadDict["relativePath"]
#                print "workloadDict:", workloadDict
            except Exception, e:
                errorMessage   = "Invalid fileEvent message received."
                self.log.error(errorMessage)
                self.log.debug("Error was: " + str(e))
                self.log.debug("workloadDict=" + str(workloadDict))
                #skip all further instructions and continue with next iteration
                continue

            #source file
            sourceFullpath = None
            try:
                #generate target filepath
                # use normpath here instead of join because relativePath is starting with a "/" and join would see that as absolut path
                sourcePath = os.path.normpath(sourcePath + os.sep + relativePath)
                sourceFullPath = os.path.join(sourcePath,filename)
                targetFullPath = os.path.normpath(self.targetPath + os.sep +  relativePath)
                self.log.debug("sourcePath: " + str (sourcePath))
                self.log.debug("filename: " + str (filename))
                self.log.debug("targetPath: " + str (targetFullPath))

            except Exception, e:
                self.log.error("Unable to generate file paths")
                trace = traceback.format_exc()
                self.log.error("Error was: " + str(trace))
                #skip all further instructions and continue with next iteration
                continue

            try:
                if self.useDataStream:
                    self.removeFile(sourceFullPath)
                else:
#                    self.copyFile(sourcePath, filename, targetFullPath)
                    self.moveFile(sourcePath, filename, targetFullPath)

                # #show filesystem statistics
                # try:
                #     self.showFilesystemStatistics(sourcePath)
                # except Exception, f:
                #     logging.warning("Unable to get filesystem statistics")
                #     logging.debug("Error was: " + str(f))

            except Exception, e:
                self.log.error("Unable to move source file: " + str (sourceFullPath) )
                trace = traceback.format_exc()
                self.log.debug("Error was: " + str(trace))
                self.log.debug("sourceFullpath="+str(sourceFullpath))
                self.log.debug("Moving source file...failed.")
                #skip all further instructions and continue with next iteration
                continue


    def copyFile(self, source, filename, target):
        maxAttemptsToCopyFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        fileWasCopied = False

        while iterationCount <= maxAttemptsToCopyFile and not fileWasCopied:
            iterationCount+=1
            try:
                # check if the directory exists before moving the file
                if not os.path.exists(target):
                    try:
                        os.makedirs(target)
                    except OSError:
                        pass
                # moving the file
                sourceFile = source + os.sep + filename
                targetFile = target + os.sep + filename
#                targetFile = "/gpfs/current/scratch_bl/test" + os.sep + filename
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                shutil.copyfile(sourceFile, targetFile)
#                subprocess.call(["mv", sourceFile, targetFile])
                fileWasCopied = True
                self.log.info("Copying file '" + str(filename) + "' from '" + str(source) + "' to '" + str(target) + "' (attempt " + str(iterationCount) + ")...success.")
            except IOError:
                self.log.debug ("IOError: " + str(filename))
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to copy file {FILE}.".format(FILE=str(source) + str(filename))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasCopied:
            self.log.info("Copying file '" + str(filename) + " from " + str(source) + " to " + str(target) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToCopyFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filename))


    def moveFile(self, source, filename, target):
        maxAttemptsToMoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        fileWasMoved = False

        while iterationCount <= maxAttemptsToMoveFile and not fileWasMoved:
            iterationCount+=1
            try:
                # check if the directory exists before moving the file
                if not os.path.exists(target):
                    try:
                        os.makedirs(target)
                    except OSError:
                        pass
                # moving the file
#                print 'paths:', source, target, os.sep, filename
                sourceFile = source + os.sep + filename
                targetFile = target + os.sep + filename
#                targetFile = "/gpfs/current/scratch_bl/test" + os.sep + filename
                self.log.debug("sourceFile: " + str(sourceFile))
                self.log.debug("targetFile: " + str(targetFile))
                try:
		    shutil.move(sourceFile, targetFile)
                    self.lastMovedFiles.append(filename)
                    fileWasMoved = True
                    self.log.info("Moving file '" + str(filename) + "' from '" + str(sourceFile) + "' to '" + str(targetFile) + "' (attempt " + str(iterationCount) + ")...success.")
                except Exception, e:
                    self.log.debug ("Checking if file was already moved: " + str(filename))
                    self.log.debug ("Error was: " + str(e))
                    if filename in self.lastMovedFiles:
                       self.log.info("File was found in history.")
                       fileWasMoved = True
                    else: 
                       self.log.info("File was not found in history.")

            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to move file {FILE}.".format(FILE=str(sourceFile))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasMoved:
            self.log.info("Moving file '" + str(filename) + " from " + str(sourceFile) + " to " + str(targetFile) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToMoveFile reached (value={ATTEMPT}). Unable to move file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filename))


    def removeFile(self, filepath):
        maxAttemptsToRemoveFile     = 2
        waitTimeBetweenAttemptsInMs = 500


        iterationCount = 0
        self.log.debug("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...")
        fileWasRemoved = False

        while iterationCount <= maxAttemptsToRemoveFile and not fileWasRemoved:
            iterationCount+=1
            try:
                os.remove(filepath)
                fileWasRemoved = True
                self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...success.")
            except Exception, e:
                trace = traceback.format_exc()
                warningMessage = "Unable to remove file {FILE}.".format(FILE=str(filepath))
                self.log.debug(warningMessage)
                self.log.debug("trace=" + str(trace))
                self.log.debug("will try again in {MS}ms.".format(MS=str(waitTimeBetweenAttemptsInMs)))

        if not fileWasRemoved:
            self.log.info("Removing file '" + str(filepath) + "' (attempt " + str(iterationCount) + ")...FAILED.")
            raise Exception("maxAttemptsToRemoveFile reached (value={ATTEMPT}). Unable to remove file '{FILE}'.".format(ATTEMPT=str(iterationCount), FILE=filepath))


    def stop(self):
        self.log.debug("Closing socket")
        self.zmqCleanerSocket.close(0)
        if not self.externalContext:
            self.log.debug("Destroying context")
            self.zmqContextForCleaner.destroy()
