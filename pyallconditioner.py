import csv
import sys
import time
import os
import fnmatch
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from datetime import datetime
from datetime import timedelta
from glob import glob
import pyall
import struct
from bisect import bisect_left, bisect_right
import sortedcollection
from operator import itemgetter
from collections import deque

###############################################################################
def main():
    parser = ArgumentParser(description='Read Kongsberg ALL file and condition the file by removing redundant records and injecting updated information to make the file self-contained.',
            epilog='Example: \n To condition a single file use -i c:/temp/myfile.all \n to condition all files in a folder use -i c:/temp/*.all\n To condition all .all files recursively in a folder, use -r -i c:/temp \n To condition all .all files recursively from the current folder, use -r -i ./ \n', formatter_class=RawTextHelpFormatter)
    parser.add_argument('-i', dest='inputFile', action='store', help='-i <ALLfilename> : Input ALL filename to image. It can also be a wildcard, e.g. *.all')
    parser.add_argument('-exclude', dest='exclude', action='store', default="", help='-exclude <datagramsID[s]> : eXclude these datagrams.  Note: this needs to be case sensitive e.g. -x YNn')
    parser.add_argument('-srh', dest='SRHInjectFileName', action='store', default="", help='-srh <filename[s]> : inJect this attitude file as A datagrams.  This will automatically remove existing A_ATTITUDE and n_NetworkAttitude datagrams. e.g. -srh "*.srh" (Hint: remember the quotes!)')
    parser.add_argument('-conditionbs', dest='conditionbs', action='store', default="", help='-conditionbs <filename> : improve the Y_SeabedImage datagrams by adding a CSV correction file. eg. -conditionbs c:\angularResponse.csv')
    parser.add_argument('-odir', dest='odir', action='store', default="", help='-odir <folder> : specify a relative output folder e.g. -odir conditioned')
    parser.add_argument('-extractbs', action='store_true', default=False, dest='extractbs', help='-extractbs : extract backscatter from Y datagram so we can analyse. [Default: False]')
    parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='-r : search Recursively.  [Default: False]')
    parser.add_argument('-svp', action='store_true', default=False, dest='svp', help='-svp : output the SVP from the sound velocity datagram.  [Default: False]')
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
        
    args = parser.parse_args()

    fileCounter=0
    matches = []
    inject = False
    extractBackscatter = False
    correctBackscatter = False
    writeConditionedFile = True
    conditionBS = False
    extractSVP = False
    latitude = 0
    longitude = 0

    
    if args.recursive:
        for root, dirnames, filenames in os.walk(os.path.dirname(args.inputFile)):
            for f in fnmatch.filter(filenames, '*.all'):
                matches.append(os.path.join(root, f))
                print (matches[-1])
    else:
        if os.path.exists(args.inputFile):
            matches.append (os.path.abspath(args.inputFile))
        else:
            for filename in glob(args.inputFile):
                matches.append(filename)
        print (matches)

    if len(matches) == 0:
        print ("Nothing found in %s to condition, quitting" % args.inputFile)
        exit()

    if len(args.exclude) > 0:
        print ("Excluding datagrams: %s :" % args.exclude)

    if len(args.conditionbs) > 0:
        if not os.path.exists(args.conditionbs):
            print ("oops: backscatter conditioning filename does not exist, please try again: %s" % args.conditionbs)
            exit()
        ARC = loadCSVFile(args.conditionbs)
        print ("Conditioning Y_SeabedImage datagrams with: %s :" % args.conditionbs)
        conditionBS = True
        args.exclude = 'Y' # we need to NOT write out the original data as we will be creating new records

    if args.extractbs:
        extractBackscatter = True
        # we need a generic set of beams into which we can insert individual ping data.  Thhis will be the angular respnse curve
        beamdetail = [0,0,0,0]
        startAngle = -90
        ARC = [pyall.cBeam(beamdetail, i) for i in range(startAngle, -startAngle)]
        beamPointingAngles = []
        transmitSector = []
        writeConditionedFile = False # we dont need to write a conditioned .all file
        outFileName = os.path.join(os.path.dirname(os.path.abspath(matches[0])), "AngularResponseCurve.csv")
        outFileName = createOutputFileName(outFileName, args.odir)

    # the user has specified a file for injection, so load it into a dictionary so we inject them into the correct spot in the file
    if len(args.SRHInjectFileName) > 0:
        inject = True
        print ("SRH Injector will strip 'n' attitude records while injecting %s" % args.SRHInjectFileName)
        print ("SRH Injector will inject system 2 'A' records as an inactive attitude data sensor with empty pitch,roll and heading datap")
        SRH = SRHReader()
        SRH.loadFiles(args.SRHInjectFileName) # load all the filenames
        print ("Records to inject: %d" % len(SRH.SRHData))
        # auto exclude attitude records
        args.exclude = 'n'

    if args.svp:
        extractSVP=True

    for filename in matches:

        if writeConditionedFile:
            # create an output file based on the input
            outFileName  = createOutputFileName(filename, args.odir)
            outFilePtr = open(outFileName, 'wb')
            print ("writing to file: %s" % outFileName)

        counter = 0
        
        r = pyall.ALLReader(filename)
        nav = r.loadNavigation()

        if inject:                    
            TypeOfDatagram, datagram = r.readDatagram()
            # kill off the leading records so we do not swamp the filewith unwanted records
            SRHSubset = deque(SRH.SRHData)
            SRHSubset = trimInjectionData(r.to_timestamp(r.currentRecordDateTime()), SRHSubset)
            r.rewind()

        while r.moreData():
            # read a datagram.  If we support it, return the datagram type and aclass for that datagram
            TypeOfDatagram, datagram = r.readDatagram()

            # read the bytes into a buffer 
            rawBytes = r.readDatagramBytes(datagram.offset, datagram.numberOfBytes)

            # before we write the datagram out, we need to inject records with a smaller from_timestamp
            if inject:                    
                if TypeOfDatagram in args.exclude:
                    # dont trigger on records we are rejecting!        
                    continue
                counter = injector(outFilePtr, r.recordDate, r.recordTime, r.to_timestamp(r.currentRecordDateTime()), SRHSubset, counter)

                # this is a testbed until we figure out how caris handles the application of heave.
                if TypeOfDatagram == 'X':
                    datagram.read()
                    # now encode the datagram back, making changes along the way
                    datagram.TransducerDepth = 999
                    dg = datagram.encode()
                    outFilePtr.write(dg)
                    continue #we do not want to write the records twice!

            if extractBackscatter:
                '''to extract backscatter angular response curve we need to keep a count and sum of all samples in a per degree sector'''
                '''to do this, we need to take into account the take off angle of each beam'''
                if TypeOfDatagram == 'N':
                    datagram.read()
                    beamPointingAngles = datagram.BeamPointingAngle
                    transmitSector = datagram.TransmitSectorNumber
                if TypeOfDatagram == 'Y':
                    datagram.read()
                    for i in range(len(datagram.beams)):
                        arcIndex = round(beamPointingAngles[i]-startAngle) #quickly find the correct slot for the data
                        ARC[arcIndex].sampleSum = ARC[arcIndex].sampleSum + sum(datagram.beams[i].samples)
                        ARC[arcIndex].sampleCount = ARC[arcIndex].sampleCount + len(datagram.beams[i].samples)
                        ARC[arcIndex].sector = transmitSector[i]
                continue
            
            if conditionBS:
                if TypeOfDatagram == 'N':
                    datagram.read()
                    beamPointingAngles = datagram.BeamPointingAngle
                    transmitSector = datagram.TransmitSectorNumber
                if TypeOfDatagram == 'Y':
                    datagram.read()
                    datagram.ARC = ARC
                    bytes = datagram.encode()
                    outFilePtr.write(bytes)

            if extractSVP:
                extractProfile(datagram, TypeOfDatagram, latitude, longitude, filename, args.odir)
        

            # the user has opted to skip this datagram, so continue
            if TypeOfDatagram in args.exclude:
                continue

            if writeConditionedFile:
                outFilePtr.write(rawBytes)

        update_progress("Processed: %s (%d/%d)" % (filename, fileCounter, len(matches)), (fileCounter/len(matches)))
        fileCounter +=1
        r.close()

    # print out the extracted backscatter angular response curve
    if extractBackscatter:
        print("Writing backscatter angular response curve to: %s" % outFileName)
        with open(outFileName, 'w') as f:
            f.write("TakeOffAngle(Deg), BackscatterAmplitude(dB), Sector, %s \n" % args.inputFile )
            for beam in ARC:
                if beam.sampleCount > 0:
                    f.write("%.3f, %.3f, %d \n" % (beam.takeOffAngle, (beam.sampleSum/beam.sampleCount)/10, beam.sector))

    update_progress("Process Complete: ", (fileCounter/len(matches)))
    if writeConditionedFile:
        print ("Saving conditioned file to: %s" % outFileName)        
        outFilePtr.close()


###############################################################################
def extractProfile(datagram, TypeOfDatagram, latitude, longitude, filename, odir):
    if (TypeOfDatagram == 'P'):
        datagram.read()
        # remember the current position, so we can use it for the SVP extraction
        latitude = datagram.Latitude
        longitude = datagram.Longitude

    if TypeOfDatagram == 'U':
        datagram.read()
        outSVP = os.path.join(os.path.dirname(os.path.abspath(filename)), "SVP.csv")
        outSVP = createOutputFileName(outSVP, args.odir)
        print("Writing SVP Profile : %s" % outSVP)
        with open(outSVP, 'w') as f:
            f.write("[SVP_Version_2]\n")
            f.write("%s\n" % filename)
            
            
            day_of_year = (r.currentRecordDateTime() - datetime(r.currentRecordDateTime().year, 1, 1)).days
            lat = decdeg2dms(latitude)
            lon = decdeg2dms(longitude)
            f.write("Section %s-%s %s:%s:%s %s:%s:%s\n" % (r.currentRecordDateTime().year, day_of_year, r.currentRecordDateTime().hour, r.currentRecordDateTime().minute, r.currentRecordDateTime().second, lat[0], lat[1], lat[2] ))
            for row in datagram.data:
                f.write("%.3f, %.3f \n" % (row[0], row[1]))
            f.close()
    return

###############################################################################
def trimInjectionData(recordTimestamp, SRHData):
    print ("Trimming unwanted records up to 1 second before start of .all first record timestamp..." )
    i=0
    while ((len(SRHData) > 0) and (recordTimestamp - SRHData[0][0]) > 1.0):
        SRHData.popleft()
        i=i+1
    
    print ("Records trimmed:%d" % i )
    return SRHData

###############################################################################
def injector(outFilePtr, recordDate, recordTime, currentRecordTimeStamp, injectionData, counter):
    '''inject data into the output file and pop the record from the injector'''
    if len(injectionData) == 0:
        return
    recordsToAdd = []
    while ((len(injectionData) > 0) and (float(injectionData[0][0]) <= currentRecordTimeStamp)):
        recordsToAdd.append(injectionData.popleft())
    
    if len(recordsToAdd) > 0:
        # counter = counter + 1
        # a = pyall.A_ATTITUDE_ENCODER()
        # datagram = a.encode(recordsToAdd, counter)
        # outFilePtr.write(datagram)

        h = pyall.H_HEIGHT_ENCODER()
        datagram = h.encode(recordsToAdd[0][1])
        outFilePtr.write(datagram)
    return counter

# ###############################################################################
# def loadSRHFile(fileName):
#     '''the SRH file format is the KOngsberg PFreeHeave binary file format'''
#     with open(fileName, 'r') as f:
#         ALLPacketHeader_fmt = '=LBBHLL'
#         ALLPacketHeader_len = struct.calcsize(ALLPacketHeader_fmt)
#         ALLPacketHeader_unpack = struct.Struct(ALLPacketHeader_fmt).unpack_from
#             reader = csv.reader(f)
#             data = list(reader)
#     return data

###############################################################################
def loadCSVFile(fileName):
    with open(fileName, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)
    return data

###############################################################################
def loadBSCorrFile(FileName):
    '''method to read a bscorr file into a list for processing.  The list will have the various modes and the corrections to apply to each sector'''
    with open(FileName, 'r') as f:
        reader = csv.reader(f)
        BSCorr = list(reader)
    return BSCorr

###############################################################################
def from_timestamp(unixtime):
    return datetime(1970, 1 ,1) + timedelta(seconds=unixtime)

###############################################################################
def decdeg2dms(dd):
   is_positive = dd >= 0
   dd = abs(dd)
   minutes,seconds = divmod(dd*3600,60)
   degrees,minutes = divmod(minutes,60)
   degrees = degrees if is_positive else -degrees
   return (degrees,minutes,seconds)
###############################################################################
def update_progress(job_title, progress):
    length = 20 # modify this to change the length
    block = int(round(length*progress))
    msg = "\r{0}: [{1}] {2}%".format(job_title, "#"*block + "-"*(length-block), round(progress*100, 2))
    if progress >= 1: msg += " DONE\r\n"
    sys.stdout.write(msg)
    sys.stdout.flush()

###############################################################################
def createOutputFileName(path, odir):
     '''Create a valid output filename. if the name of the file already exists the file name is auto-incremented.'''
     path      = os.path.expanduser(path)

     if not os.path.exists(path):
        return path

     root, ext = os.path.splitext(os.path.expanduser(path))
     dir       = os.path.dirname(root)
     fname     = os.path.basename(root)
     candidate = fname+ext
     index     = 1
     ls        = set(os.listdir(dir))
     while candidate in ls:
             candidate = "{}_{}{}".format(fname,index,ext)
             index    += 1
     if not os.path.exists(os.path.join(dir, odir)):
         os.makedirs(os.path.join(dir, odir))

     return os.path.join(dir, odir, candidate)


###############################################################################
class SRHReader:
    '''class to read a Kongsberg SRH PFreeHeave file'''
    '''This class may need to read multiple SRH files, merge them, sort and provide rapid access using the bisect tools in python'''
    def __init__(self):
        self.SRHPacket_fmt = '>HBBLHhBH'  #pfreeheave is big endian format
        self.SRHPacket_len = struct.calcsize(self.SRHPacket_fmt)
        self.SRHPacket_unpack = struct.Struct(self.SRHPacket_fmt).unpack_from
        self.SRHData = deque()

    def loadFiles(self, filename):
        matches = []
        if os.path.exists(filename):
            matches.append (os.path.abspath(filename))
        else:
            for f in sorted(glob(filename)):
                matches.append(f)
        print (matches)

        if len(matches) == 0:
            print ("Nothing found in %s to condition, quitting" % filename)
            exit()
        print ("Loading SRH Files:")
        for f in matches:
            self.loadfile(f)
        return

    def loadfile(self, filename):

        if not os.path.isfile(filename):
            print ("SRH file not found:", filename)
            return
        fileptr = open(filename, 'rb')        
        fileSize = os.path.getsize(filename)
        # self.sc = sortedcollection.SortedCollection(key=itemgetter(0))
        # numberRecords = int(fileSize / self.SRHPacket_len)
        print (filename)
        try:
            while True:
                data = fileptr.read(self.SRHPacket_len)
                if not data: break
                s = self.SRHPacket_unpack(data)
                timestamp = float(s[3]) + (s[4] * 0.0001)
                heave = float(s[5]) * 0.01
                self.SRHData.append([timestamp, heave])
                # print (from_timestamp(timestamp), heave)
        except struct.error:
            print ("Exception loading SRH file.  Will process as much as can be read")

    # def swap32(self, i):
    #     return struct.unpack("<I", struct.pack(">I", i))[0]
    # def swap16(self, i):
    #     return struct.unpack("<H", struct.pack(">H", i))[0]

# def swap32(self, x):
#         return int.from_bytes(x.to_bytes(4, byteorder='little'), byteorder='big', signed=False)
###############################################################################
if __name__ == "__main__":
    start_time = time.time() # time  the process
    main()
    print("Duration: %d seconds" % (time.time() - start_time))
