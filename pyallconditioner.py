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

def main():
    parser = ArgumentParser(description='Read Kongsberg ALL file and condition the file by removing redundant records and injecting updated information to make the file self-contained.',
            epilog='Example: \n To condition a single file use -i c:/temp/myfile.all \n to condition all files in a folder use -i c:/temp/*.all\n To condition all .all files recursively in a folder, use -r -i c:/temp \n To condition all .all files recursively from the current folder, use -r -i ./ \n', formatter_class=RawTextHelpFormatter)
    parser.add_argument('-i', dest='inputFile', action='store', help='-i <ALLfilename> : input ALL filename to image. It can also be a wildcard, e.g. *.all')
    parser.add_argument('-x', dest='exclude', action='store', default="", help='-x <datagramsID[s]> : exclude these datagrams.  Note: this needs to be case sensitive e.g. -x YNn')
    parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='-r : search recursively.  [Default: False]')
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
        
    args = parser.parse_args()

    fileCounter=0
    matches = []
        
    if args.recursive:
        for root, dirnames, filenames in os.walk(os.path.dirname(args.inputFile)):
            for f in fnmatch.filter(filenames, '*.all'):
                matches.append(os.path.join(root, f))
                print (matches[-1])
    else:
        for filename in glob(args.inputFile):
            matches.append(filename)
        print (matches)
    if len(matches) == 0:
        print ("Nothing found in %s to convert, quitting" % args.inputFile)
        exit()

    for filename in matches:

        # create an output file based on the input
        outFileName  = createOutputFileName(filename)
        outFilePtr = open(outFileName, 'bw')

        # print ("processing file: %s" % filename)
        lastTimeStamp = 0
        trackRecordCount = 0
        line_parts = []
        line = []
        
        r = pyall.ALLReader(filename)
        start_time = time.time() # time  the process

        while r.moreData():
            # read a datagram.  If we support it, return the datagram type and aclass for that datagram
            # The user then needs to call the read() method for the class to undertake a fileread and binary decode.  This keeps the read super quick.
            TypeOfDatagram, datagram = r.readDatagram()

            if TypeOfDatagram not in args.exclude:
                # read the bytes into a buffer 
                rawBytes = r.readDatagramBytes(datagram.offset, datagram.bytes)
                # write the bytes to the new file
                outFilePtr.write(rawBytes)

            # if TypeOfDatagram == 'I':
            #     datagram.read()
            #     print (datagram.installationParameters)
            #     #  print ("Lat: %.5f Lon: %.5f" % (datagram.Latitude, datagram.Longitude))
            # if TypeOfDatagram == 'D':
            #     datagram.read()
            #     nadirBeam = int(datagram.NBeams / 2)
            #     # print (("Nadir Depth: %.3f AcrossTrack %.3f TransducerDepth %.3f Checksum %s" % (datagram.Depth[nadirBeam], datagram.AcrossTrackDistance[nadirBeam], datagram.TransducerDepth, datagram.checksum)))
            # if TypeOfDatagram == 'X':
            #     datagram.read()
            #     # nadirBeam = int(datagram.NBeams / 2)
            #     # print (("Nadir Depth: %.3f AcrossTrack %.3f TransducerDepth %.3f" % (datagram.Depth[nadirBeam], datagram.AcrossTrackDistance[nadirBeam], datagram.TransducerDepth)))
            #     pingCount += 1

        update_progress("Processed: %s (%d/%d)" % (filename, fileCounter, len(matches)), (fileCounter/len(matches)))
        # lastTimeStamp = update[0]
        fileCounter +=1
        r.close()
        
    update_progress("Process Complete: ", (fileCounter/len(matches)))
    print ("Saving conditioned file to: %s" % outFileName)        
    outFilePtr.close()

def from_timestamp(unixtime):
    return datetime(1970, 1 ,1) + timedelta(seconds=unixtime)

def update_progress(job_title, progress):
    length = 20 # modify this to change the length
    block = int(round(length*progress))
    msg = "\r{0}: [{1}] {2}%".format(job_title, "#"*block + "-"*(length-block), round(progress*100, 2))
    if progress >= 1: msg += " DONE\r\n"
    sys.stdout.write(msg)
    sys.stdout.flush()

# Create a valid output filename. if the name of the file already exists the file name is auto-incremented.
def createOutputFileName(path):
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

if __name__ == "__main__":
    main()

