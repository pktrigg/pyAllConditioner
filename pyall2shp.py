import sys
import time
import os
import fnmatch
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from datetime import datetime
from datetime import timedelta
from glob import glob
# local imports
import pyall
import shapefile

def main():
    parser = ArgumentParser(description='Read Kongsberg ALL file and create an ESRI shape file of the trackplot.',
            epilog='Example: \n To convert a single file use -i c:/temp/myfile.all \n to convert all files in a folder use -i c:/temp/*.all\n To convert all .all files recursively in a folder, use -r -i c:/temp \n To convert all .all files recursively from the current folder, use -r -i ./ \n', formatter_class=RawTextHelpFormatter)
    parser.add_argument('-i', dest='inputFile', action='store', help='-i <ALLfilename> : input ALL filename to image. It can also be a wildcard, e.g. *.all')
    parser.add_argument('-o', dest='outputFile', action='store', default='track.shp', help='-o <SHPfilename.shp> : output filename to create. e.g. trackplot.shp [Default: track.shp]')
    parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='-r : search recursively.  [Default: False]')
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
        
    args = parser.parse_args()
    # we need to remember the previous record so we only create uniq values, not duplicates
    fileOut = args.outputFile #"track.shp"
    if not fileOut.lower().endswith('.shp'):
        fileOut += '.shp'

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
        print ("Nothing found to convert, quitting")
        exit()

    if os.path.isfile(fileOut):
        try:
            # Create a shapefile reader
            r = shapefile.Reader(fileOut)
            # Create a shapefile writer
            # using the same shape type
            # as our reader
            w = shapefile.Writer(r.shapeType)
            # Copy over the existing dbf fields
            w.fields = list(r.fields)
            # Copy over the existing dbf records
            w.records.extend(r.records())
            # Copy over the existing polygons
            w._shapes.extend(r.shapes())
        except shapefile.error:
            print ("Problem opening existing shape file, aborting!")
            exit()
    else:
        # w = shapefile.Writer(shapefile.POINTZ)
        w = shapefile.Writer(shapefile.POLYLINE)
        w.autoBalance = 1
        w.field("LineName", "C")
        # w.field("WaterDepth", "N")
        w.field("UNIXTime", "N")
        w.field("SurveyDate", "D")
        
    for filename in matches:
        # print ("processing file: %s" % filename)
        lastTimeStamp = 0
        trackRecordCount = 0
        line_parts = []
        line = []
        
        r = pyall.ALLReader(filename)
        start_time = time.time() # time  the process
        navigation = r.loadNavigation()
        for update in navigation:
            if update[0] - lastTimeStamp > 30:
                line.append([float(update[2]),float(update[1])])
                # trackRecordCount += 1
                lastTimeStamp = update[0]
        # now add the very last update
        line.append([float(navigation[-1][2]),float(navigation[-1][1])])
            
        line_parts.append(line)
        w.line(parts=line_parts)
        # now add to the shape file.
        recTimeStamp = from_timestamp(navigation[0][0]).strftime("%Y/%m/%d %H:%M:%S")
        recDate = from_timestamp(navigation[0][0]).strftime("%Y%m%d")
        depth = 1111.123
        w.record(os.path.basename(filename), int(navigation[0][0]), recDate) 
        # w.record(os.path.basename(filename), depth, navigation[0][0], recDate) 

        update_progress("Processed: %s (%d/%d)" % (filename, fileCounter, len(matches)), (fileCounter/len(matches)))
        lastTimeStamp = update[0]
        fileCounter +=1
        r.close()
        
    update_progress("Process Complete: ", (fileCounter/len(matches)))
    print ("Saving shapefile: %s" % fileOut)        
    w.save(fileOut)

    # now write out a prj file so the data has a spatial Reference
    f = open(fileOut.replace('.shp','.prj'), 'w')
    f.write('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]') # python will convert \n to os.linesep
    f.close() # you can omit in most cases as the destructor will call it
    

def from_timestamp(unixtime):
    return datetime(1970, 1 ,1) + timedelta(seconds=unixtime)

def update_progress(job_title, progress):
    length = 20 # modify this to change the length
    block = int(round(length*progress))
    msg = "\r{0}: [{1}] {2}%".format(job_title, "#"*block + "-"*(length-block), round(progress*100, 2))
    if progress >= 1: msg += " DONE\r\n"
    sys.stdout.write(msg)
    sys.stdout.flush()

if __name__ == "__main__":
    main()

