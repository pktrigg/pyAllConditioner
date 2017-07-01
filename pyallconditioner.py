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

###############################################################################
def main():
    parser = ArgumentParser(description='Read Kongsberg ALL file and condition the file by removing redundant records and injecting updated information to make the file self-contained.',
            epilog='Example: \n To condition a single file use -i c:/temp/myfile.all \n to condition all files in a folder use -i c:/temp/*.all\n To condition all .all files recursively in a folder, use -r -i c:/temp \n To condition all .all files recursively from the current folder, use -r -i ./ \n', formatter_class=RawTextHelpFormatter)
    parser.add_argument('-i', dest='inputFile', action='store', help='-i <ALLfilename> : Input ALL filename to image. It can also be a wildcard, e.g. *.all')
    parser.add_argument('-exclude', dest='exclude', action='store', default="", help='-exclude <datagramsID[s]> : eXclude these datagrams.  Note: this needs to be case sensitive e.g. -x YNn')
    parser.add_argument('-srh', dest='SRHInjectFileName', action='store', default="", help='-srh <filename> : inJect this attitude file as A datagrams.  This will automatically remove existing A_ATTITUDE and n_NetworkAttitude datagrams. e.g. -srh delayedHeave.srh')
    parser.add_argument('-conditionbs', dest='conditionbs', action='store', default="", help='-conditionbs <filename> : improve the Y_SeabedImage datagrams by adding a CSV correction file. eg. -conditionbs c:\angularResponse.csv')
    parser.add_argument('-extractbs', action='store_true', default=False, dest='extractbs', help='-extractbs : extract backscatter from Y datagram so we can analyse. [Default: False]')
    parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='-r : search Recursively.  [Default: False]')
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
        backscatterConditionData = loadCSVFile(args.conditionbs)
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
        outFileName = createOutputFileName(outFileName)

    # the user has specified a file for injection, so load it into a dictionary so we inject them into the correct spot in the file
    if len(args.SRHInjectFileName) > 0:
        if not os.path.exists(args.SRHInjectFileName):
            print ("oops: Injection filename does not exist, please try again: %s" % args.SRHInjectFileName)
            exit()
        injectionData = loadCSVFile(args.SRHInjectFileName)
        inject = True
        # auto exclude attitude records
        args.exclude = 'nA'
        print ("SRH Injector will strip 'n' and 'A' attitude records while injecting %s" % args.SRHInjectFileName)

    for filename in matches:

        if writeConditionedFile:
            # create an output file based on the input
            outFileName  = createOutputFileName(filename)
            outFilePtr = open(outFileName, 'bw')
            print ("writing to file: %s" % outFileName)

        counter = 0
        
        r = pyall.ALLReader(filename)
        start_time = time.time() # time  the process

        while r.moreData():
            # read a datagram.  If we support it, return the datagram type and aclass for that datagram
            TypeOfDatagram, datagram = r.readDatagram()

            # read the bytes into a buffer 
            rawBytes = r.readDatagramBytes(datagram.offset, datagram.numberOfBytes)

            # before we write the datagram out, we need to inject records with a smaller from_timestamp
            if inject:    
                counter = injector(outFilePtr, r.recordDate, r.recordTime, r.to_timestamp(r.currentRecordDateTime()), injectionData, counter)

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

            if conditionBS:
                if TypeOfDatagram == 'N':
                    datagram.read()
                    beamPointingAngles = datagram.BeamPointingAngle
                    transmitSector = datagram.TransmitSectorNumber
                if TypeOfDatagram == 'Y':
                    datagram.read()
                    bytes = datagram.encode()
                    outFilePtr.write(bytes)
                    
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
def injector(outFilePtr, recordDate, recordTime, currentRecordTimeStamp, injectionData, counter):
    '''inject data into the output file and pop the record from the injector'''
    if len(injectionData) == 0:
        return
    recordsToAdd = []
    while ((len(injectionData) > 0) and (float(injectionData[0][0]) <= currentRecordTimeStamp)):
        recordsToAdd.append(injectionData.pop(0))
    
    if len(recordsToAdd) > 0:
        counter = counter + 1
        a = pyall.A_ATTITUDE_ENCODER()
        datagram = a.encode(recordsToAdd, counter)
        outFilePtr.write(datagram)
    return counter

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
def update_progress(job_title, progress):
    length = 20 # modify this to change the length
    block = int(round(length*progress))
    msg = "\r{0}: [{1}] {2}%".format(job_title, "#"*block + "-"*(length-block), round(progress*100, 2))
    if progress >= 1: msg += " DONE\r\n"
    sys.stdout.write(msg)
    sys.stdout.flush()

###############################################################################
def createOutputFileName(path):
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
     return os.path.join(dir,candidate)

###############################################################################
if __name__ == "__main__":
    main()