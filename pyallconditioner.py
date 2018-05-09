#name:		  pyALLConditioner
#created:	   April 2017
#by:			p.kennedy@fugro.com
#description:   python module to pre-process a Kongsberg ALL sonar file and do useful hings with it
#			   See readme.md for more details
# more changes

import csv
import sys
import time
import math
import os
import fnmatch
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from datetime import datetime
from datetime import timedelta
from glob import glob
import pyall
import struct
# from bisect import bisect_left, bisect_right
import sortedcollection
from operator import itemgetter
from collections import deque
from collections import defaultdict

###############################################################################
def main():
	parser = ArgumentParser(description='Read Kongsberg ALL file and condition the file by removing redundant records and injecting updated information to make the file self-contained.',
			epilog='Example: \n To condition a single file use -i c:/temp/myfile.all \n to condition all files in a folder use -i c:/temp/*.all\n To condition all .all files recursively in a folder, use -r -i c:/temp \n To condition all .all files recursively from the current folder, use -r -i ./ \n', formatter_class=RawTextHelpFormatter)
	parser.add_argument('-i', dest='inputFile', action='store', help='Input ALL filename to image. It can also be a wildcard, e.g. *.all')
	parser.add_argument('-exclude', dest='exclude', action='store', default="", help='Exclude these datagrams.  Note: this needs to be case sensitive e.g. -x YNn')
	parser.add_argument('-srh', dest='SRHInjectFileName', action='store', default="", help='Inject this attitude file as A datagrams. e.g. -srh "*.srh" (Hint: remember the quotes!)')
	parser.add_argument('-conditionbs', dest='conditionbs', action='store', default="", help='Improve the Y_SeabedImage datagrams by adding a CSV correction file. eg. -conditionbs c:/angularResponse.csv')
	parser.add_argument('-odir', dest='odir', action='store', default="", help='Specify a relative output folder e.g. -odir conditioned')
	parser.add_argument('-extractbs', action='store_true', default=False, dest='extractbs', help='Extract backscatter from Y datagram so we can analyse. [Default: False]')
	parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='Search Recursively from the current folder.  [Default: False]')
	parser.add_argument('-svp', action='store_true', default=False, dest='svp', help='Output a CARIS compatible SVP file based on the sound velocity datagram.  [Default: False]')
	parser.add_argument('-attitude', action='store_true', default=False, dest='attitude', help='Output a CSV file of attitude.  [Default: False]')
	parser.add_argument('-nadir', action='store_true', default=False, dest='nadir', help='Output a CSV file of the nadir beam.  [Default: False]')
	parser.add_argument('-install', action='store_true', default=False, dest='install', help='Output the installation parameters to a CSV. [Default: False]')
	parser.add_argument('-bscorr', action='store_true', default=False, dest='bscorr', help='Output the backscatter bscorr.txt file as used in the PU.  This is useful for backscatter calibration and processing, and removes the need to telnet into the PU.   [Default: False]')
	parser.add_argument('-splitd', action='store_true', default=False, dest='splitd', help='split the .all file every time the depth mode changes.  [Default: False]')
	parser.add_argument('-splitt', dest='splitt', action='store', default="", help='Split the .all file based on time inm secnds e.g. -splitt 60')

	if len(sys.argv)==1:
		parser.print_help()
		sys.exit(1)
		
	args = parser.parse_args()

	fileCounter=0
	matches = []
	inject = False
	extractAttitude=False
	extractNadir=False
	extractBackscatter = False
	correctBackscatter = False
	writeConditionedFile = True
	conditionBS = False
	extractSVP = False
	extractBSCorr = False
	splitd=False
	install=False
	latitude = 0
	longitude = 0
	splitt = 0
	splitfileend = 0

	if args.recursive:
		for root, dirnames, filenames in os.walk(os.path.dirname(args.inputFile)):
			for f in fnmatch.filter(filenames, '*.all'):
				matches.append(os.path.join(root, f))
				# print (matches[-1])
	else:
		if os.path.exists(args.inputFile):
			matches.append (os.path.abspath(args.inputFile))
		else:
			for filename in glob(args.inputFile):
				matches.append(filename)
		# print (matches)

	if len(matches) == 0:
		print ("Nothing found in %s to condition, quitting" % args.inputFile)
		exit()

	if len(args.splitt) > 0:
		splitt = int(args.splitt)
		print ("Splitting on time interval: %s :" % splitt)

	if len(args.exclude) > 0:
		print ("Excluding datagrams: %s :" % args.exclude)

	if len(args.conditionbs) > 0:
		beamPointingAngles = []
		ARC = loadARC(args.conditionbs)		
		conditionBS = True
		args.exclude = 'Y' # we need to NOT write out the original data as we will be creating new records

	if args.extractbs:
		extractBackscatter = True
		writeConditionedFile= False #we do not need to write out a .all file
		# we need a generic set of beams into which we can insert individual ping data.  Thhis will be the angular respnse curve
		beamdetail = [0,0,0,0]
		startAngle = -90
		ARC = [pyall.cBeam(beamdetail, i) for i in range(startAngle, -startAngle)]
		beamPointingAngles = []
		transmitSector = []
		writeConditionedFile = False # we dont need to write a conditioned .all file
		outFileName = os.path.join(os.path.dirname(os.path.abspath(matches[0])), args.odir, "AngularResponseCurve.csv")
		outFileName = createOutputFileName(outFileName)

	# the user has specified a file for injection, so load it into a dictionary so we inject them into the correct spot in the file
	if len(args.SRHInjectFileName) > 0:
		inject = True
		print ("SRH Injector will strip 'n' attitude records while injecting %s" % args.SRHInjectFileName)
		print ("SRH Injector will inject system 2 'A' records as an inactive attitude data sensor with empty pitch,roll and heading datap")
		SRH = SRHReader()
		SRH.loadFiles(args.SRHInjectFileName) # load all the filenames
		print ("Records to inject: %d" % len(SRH.SRHData))
		# auto exclude attitude records.  on reflection, we should probably NOT do this.
		# args.exclude = 'n'

	if args.install:
		install=True
		writeConditionedFile= False #we do not need to write out a .all file
		r = pyall.ALLReader(matches[0])
		installStart, installStop, initialMode, datagram = r.loadInstallationRecords()
		r.close()
		# header = "Waterline(m),Transmit X(m), Transmit Y(m), Transmit Z(m), Receive(X), Receive(Y), Receive(Z), "
		# print(header)
		header = "Filename"
		for code in datagram.installationParameters:
			header = header + "," + InstallationCodeToText(code) + " (" + code + ")"
		print (header)

	if args.attitude:
		extractAttitude=True
		writeConditionedFile= False #we do not need to write out a .all file

	if args.nadir:
		extractNadir=True
		writeConditionedFile= False #we do not need to write out a .all file
		
	if args.bscorr:
		extractBSCorr=True
		writeConditionedFile= False #we do not need to write out a .all file

	if args.svp:
		extractSVP=True
		writeConditionedFile= False #we do not need to write out a .all file

	if args.splitd:
		splitd=True
		initialDepthMode = ""

	for filename in matches:
		if extractAttitude:
			# create an output file based on the input
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName = os.path.splitext(outFileName)[0]+'.txt'
			outFileName  = createOutputFileName(outFileName,".txt")
			outFilePtr = open(outFileName, 'w')
			print ("writing to file: %s" % outFileName)

		if writeConditionedFile:
			# create an output file based on the input
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName  = createOutputFileName(outFileName)
			outFilePtr = open(outFileName, 'wb')
			print ("writing to file: %s" % outFileName)

		# open the file and do some initialisation stuff 
		r = pyall.ALLReader(filename)
		counter = 0

		if extractNadir:
			print ("NadirDepth,TxDepth, Pitch,Heave,Roll ")
		
		if extractAttitude:
			# read the first record so we get a date for the file header
			typeOfDatagram, datagram = r.readDatagram()
			str = r.currentRecordDateTime().strftime('%Y%m%d')
			outFilePtr.write("Name:" + os.path.basename(filename) + "," + str + "\n")

		if splitd:
			InstallStart, InstallEnd, initialDepthMode = r.loadInstallationRecords()
		if splitt > 0:
			InstallStart, InstallEnd, initialDepthMode = r.loadInstallationRecords()

		if inject:					
			TypeOfDatagram, datagram = r.readDatagram()
			# kill off the leading records so we do not swamp the filewith unwanted records
			SRHSubset = deque(SRH.SRHData)
			SRHSubset = trimInjectionData(pyall.to_timestamp(r.currentRecordDateTime()), SRHSubset)
			r.rewind()

		if extractSVP:
			# we need the position of the SVP dip in the SVP file, so use the first position record in the file
			nav = r.loadNavigation(True)
			if len(nav) > 0:
				latitude = nav[0][1]
				longitude = nav[0][2]

		currPitch = 0
		currRoll = 0
		currHeave = 0

		while r.moreData():
			# read a datagram.  If we support it, return the datagram type and aclass for that datagram
			TypeOfDatagram, datagram = r.readDatagram()

			if splitfileend == 0:
				splitfileend = pyall.to_timestamp(r.currentRecordDateTime())

			# read the bytes into a buffer 
			rawBytes = r.readDatagramBytes(datagram.offset, datagram.numberOfBytes)

			if extractNadir:
				if TypeOfDatagram == 'D' or  TypeOfDatagram == 'X':
					datagram.read()
					# find the depth nearest to Nadir
					# https://stackoverflow.com/questions/9706041/finding-index-of-an-item-closest-to-the-value-in-a-list-thats-not-entirely-sort
					nadirBeam = min(range(len(datagram.AcrossTrackDistance)), key=lambda i: abs(datagram.AcrossTrackDistance[i]))
					print ("%.3f,%.3f,%.3f,%.3f,%.3f" % (datagram.Depth[nadirBeam], datagram.TransducerDepth/100, currRoll, currPitch, currHeave))
				if TypeOfDatagram == 'A':
					datagram.read()
					currRoll = datagram.Attitude[-1][3]
					currPitch = datagram.Attitude[-1][4]
					currHeave = datagram.Attitude[-1][5]
			if extractAttitude:
				if TypeOfDatagram == 'A':
					datagram.read()
					for a in datagram.Attitude:
						dateobject = pyall.to_DateTime(a[0], a[1])
						str = ("%d,%.3f,%.3f,%.3f,%.3f\n" % (a[0],a[1],a[3],a[4],a[5]))
						outFilePtr.write(str)
						# outFilePtr.write("%s,%.3f" % (pyall.to_timestamp(dateobject), a[5]))
					currRoll = datagram.Attitude[len(datagram.Attitude)[3]]
					currPitch = datagram.Attitude[len(datagram.Attitude)[4]]
					currHeave = datagram.Attitude[len(datagram.Attitude)[5]]
			if install:
				if TypeOfDatagram == 'I':
					datagram.read()
					row = filename
					for i in datagram.installationParameters :
						if len(datagram.installationParameters[i]) == 0:
							datagram.installationParameters[i] = "0.00"
						row = row + "," + datagram.installationParameters[i]
						# row.replace(",,",",")
					print (row)
					break
			if splitt:
					# datagram.read()
					if pyall.to_timestamp(r.currentRecordDateTime()) > (splitfileend + splitt):
						# write out the closing install record then close the file
						print ("closing the file as the duration has exceeded the split time")
						outFilePtr.write(InstallEnd)
						outFilePtr.close()
						
						outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
						outFileName  = createOutputFileName(outFileName)
						outFilePtr = open(outFileName, 'wb')
						print ("writing to split file: %s" % outFileName)
						outFilePtr.write(InstallStart)
						outFilePtr.write(rawBytes)
						splitfileend = pyall.to_timestamp(r.currentRecordDateTime())
			if splitd:
				if TypeOfDatagram == 'R':
					datagram.read()
					if initialDepthMode is not datagram.DepthMode:
						# write out the closing install record then close the file
						print ("closing the file as the depth mode has changed")
						outFilePtr.write(InstallEnd)
						outFilePtr.close()
						
						outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.splitext(filename)[0] + "_" + datagram.DepthMode + "." + os.path.splitext(filename)[1],)
						outFileName  = createOutputFileName(outFileName)
						outFilePtr = open(outFileName, 'wb')
						print ("writing to split file: %s" % outFileName)
						outFilePtr.write(InstallStart)
						outFilePtr.write(rawBytes)
						initialDepthMode = datagram.DepthMode #remember the new depth mode!
						
			# before we write the datagram out, we need to inject records with a smaller from_timestamp
			if inject:					
				if TypeOfDatagram in args.exclude:
					# dont trigger on records we are rejecting!		
					continue
				counter = injector(outFilePtr, pyall.to_timestamp(r.currentRecordDateTime()), SRHSubset, counter)

				# this is a testbed until we figure out how caris handles the application of heave.
				# if TypeOfDatagram == 'X':
				#	 datagram.read()
				#	 # now encode the datagram back, making changes along the way
				#	 datagram.TransducerDepth = 999
				#	 dg = datagram.encode()
				#	 outFilePtr.write(dg)
				#	 continue #we do not want to write the records twice!

			if extractBackscatter:
				'''to extract backscatter angular response curve we need to keep a count and sum of all samples in a per degree sector'''
				'''to do this, we need to take into account the take off angle of each beam'''
				if TypeOfDatagram == 'N':
					datagram.read()
					beamPointingAngles = datagram.BeamPointingAngle
					transmitSector = datagram.TransmitSectorNumber
				if TypeOfDatagram == 'Y':
					if len(beamPointingAngles)==0:
						continue #we dont yet have any raw ranges so we dont have a beam pattern so skip
					datagram.read()
					for i in range(len(datagram.beams)):
						arcIndex = round(beamPointingAngles[i]-startAngle) #quickly find the correct slot for the data
						ARC[arcIndex].sampleSum = ARC[arcIndex].sampleSum + sum(datagram.beams[i].samples)
						ARC[arcIndex].numberOfSamplesPerBeam = ARC[arcIndex].numberOfSamplesPerBeam + len(datagram.beams[i].samples)
						ARC[arcIndex].sector = transmitSector[i]
				continue
			
			if conditionBS:
				if TypeOfDatagram == 'N':
					datagram.read()
					beamPointingAngles = datagram.BeamPointingAngle
				if TypeOfDatagram == 'Y':
					if len(beamPointingAngles)==0:
						continue #we dont yet have any raw ranges so we dont have a beam pattern so skip
					datagram.read()
					datagram.ARC = ARC
					datagram.BeamPointingAngle = beamPointingAngles
					bytes = datagram.encode()
					outFilePtr.write(bytes)

			if extractSVP:
				extractProfile(datagram, TypeOfDatagram, r.currentRecordDateTime(), latitude, longitude, filename, args.odir)

			if extractBSCorr:
				extractBSCorrData(datagram, TypeOfDatagram, filename, args.odir)
		

			# the user has opted to skip this datagram, so continue
			if TypeOfDatagram in args.exclude:
				continue

			if writeConditionedFile:
				outFilePtr.write(rawBytes)

		# update_progress("Processed: %s (%d/%d)" % (filename, fileCounter, len(matches)), (fileCounter/len(matches)))
		fileCounter +=1
		r.close()

	# print out the extracted backscatter angular response curve
	if extractBackscatter:
		print("Writing backscatter angular response curve to: %s" % outFileName)
		
		# compute the mean response across the swath
		responseSum = 0
		responseCount = 0
		for beam in ARC:
			if beam.numberOfSamplesPerBeam > 0:
				responseSum = responseSum = (beam.sampleSum/10) #tenths of a dB
				responseCount = responseCount = beam.numberOfSamplesPerBeam
		responseAverage = responseSum/responseCount

		with open(outFileName, 'w') as f:
			# write out the backscatter response curve
			f.write("TakeOffAngle(Deg), BackscatterAmplitude(dB), Sector, SampleSum, SampleCount, Correction, %s \n" % args.inputFile )
			for beam in ARC:
				if beam.numberOfSamplesPerBeam > 0:
					beamARC = (beam.sampleSum/beam.numberOfSamplesPerBeam)
					f.write("%.3f, %.3f, %d, %d, %d, %.3f\n" % (beam.takeOffAngle, beamARC, beam.sector, beam.sampleSum, beam.numberOfSamplesPerBeam , beamARC + responseAverage))

	# update_progress("Process Complete: ", (fileCounter/len(matches)))
	if writeConditionedFile:
		print ("Saving conditioned file to: %s" % outFileName)		
		outFilePtr.close()


def InstallationCodeToText(code):

	allcodes = defaultdict(str)

	allcodes['WLZ'] = 'Water line vertical location in m'
	allcodes['SMH'] = 'System main head serial number'
	allcodes['HUN'] = 'Hull Unit'
	allcodes['HUT'] = 'Hull Unit tilt offset'
	allcodes['TXS'] = 'TX serial number'
	allcodes['T2X'] = 'TX no. 2 serial number'
	allcodes['R1S'] = 'RX no. 1 serial number'
	allcodes['R2S'] = 'RX no. 2 serial number'
	allcodes['STC'] = 'System transducer configuration'
	allcodes['S0Z'] = 'Transducer 0 vertical location in m'
	allcodes['S0X'] = 'Transducer 0 along location in m'
	allcodes['S0Y'] = 'Transducer 0 athwart location in m'
	allcodes['S0H'] = 'Transducer 0 heading in degrees'
	allcodes['S0R'] = 'Transducer 0 roll in degrees re horizontal'
	allcodes['S0P'] = 'Transducer 0 pitch in degrees'
	allcodes['S1Z'] = 'Transducer 1 vertical location in m'
	allcodes['S1X'] = 'Transducer 1 along location in m'
	allcodes['S1Y'] = 'Transducer 1 athwart location in m'
	allcodes['S1H'] = 'Transducer 1 heading in degrees'
	allcodes['S1R'] = 'Transducer 1 roll in degrees re horizontal'
	allcodes['S1P'] = 'Transducer 1 pitch in degrees'
	allcodes['S1N'] = 'Transducer 1 no of modules'
	allcodes['S2Z'] = 'Transducer 2 vertical location in m'
	allcodes['S2X'] = 'Transducer 2 along location in m'
	allcodes['S2Y'] = 'Transducer 2 athwart location in m'
	allcodes['S2H'] = 'Transducer 2 heading in degrees'
	allcodes['S2R'] = 'Transducer 2 roll in degrees re horizontal'
	allcodes['S2P'] = 'Transducer 2 pitch in degrees'
	allcodes['S2N'] = 'Transducer 2 no of modules'
	allcodes['S2Z'] = 'Transducer 3 vertical location in m'
	allcodes['S3Z'] = 'Transducer 3 along location in m'
	allcodes['S3Y'] = 'Transducer 3 athwart location in m'
	allcodes['S2H'] = 'Transducer 3 heading in degrees'
	allcodes['S3R'] = 'Transducer 3 roll in degrees re horizontal'
	allcodes['S3P'] = 'Transducer 3 pitch in degrees'
	allcodes['S1S'] = 'TX array size (0=0.5 1=1 2=2)'
	allcodes['S2S'] = 'RX array size (1=1 2=2)'
	allcodes['GO1'] = 'System (sonar head 1) gain offset'
	allcodes['GO2'] = 'Sonar head 2 gain offset'
	allcodes['OBO'] = 'Outer beam offset'
	allcodes['FGD'] = 'High/Low Frequency Gain Difference'
	allcodes['TSV'] = 'Transmitter (sonar head no1) software version'
	allcodes['RSV'] = 'Receiver (sonar head 2) software version'
	allcodes['BSV'] = 'BSP software version'
	allcodes['PSV'] = 'Processing unit software version'
	allcodes['DDS'] = 'DDS software version'
	allcodes['OSV'] = 'Operator station software version'
	allcodes['DSV'] = 'Datagram format version'
	allcodes['DSX'] = 'Depth (pressure) sensor along location in m'
	allcodes['DSY'] = 'Depth (pressure) sensor athwart location in m'
	allcodes['DSZ'] = 'Depth (pressure) sensor vertical location in m'
	allcodes['DSD'] = 'Depth (pressure) sensor time delay in millisec'
	allcodes['DSO'] = 'Depth (pressure) sensor offset'
	allcodes['DSF'] = 'Depth (pressure) sensor scale factor'
	allcodes['DSH'] = 'Depth (pressure) sensor heave'
	allcodes['APS'] = 'Active position system number'
	allcodes['P1Q'] = 'Position system 1 quality check of position 0:off 1:on'
	allcodes['P1M'] = 'Position system 1 motion compensation'
	allcodes['P1T'] = 'Position system 1 time stamp used'
	allcodes['P1Z'] = 'Position system 1 vertical location in m'
	allcodes['P1X'] = 'Position system 1 along location in m'
	allcodes['P1Y'] = 'Position system 1 athwart location in m'
	allcodes['P1D'] = 'Position system 1 time delay in seconds'
	allcodes['P1G'] = 'Position system 1 geodetic datum'
	allcodes['P2Q'] = 'Position system 2 quality check of position 0:off 1:on'
	allcodes['P2M'] = 'Position system 2 motion compensation'
	allcodes['P2T'] = 'Position system 2 time stamp use'
	allcodes['P2Z'] = 'Position system 2 vertical location in m'
	allcodes['P2X'] = 'Position system 2 along location in m'
	allcodes['P2Y'] = 'Position system 2 athwart location in m'
	allcodes['P2D'] = 'Position system 2 time delay in seconds'
	allcodes['P2G'] = 'Position system 2 geodetic datum'
	allcodes['P3Q'] = 'Position system 3 quality check of position 0:off 1:on'
	allcodes['P3M'] = 'Position system 3 motion compensation'
	allcodes['P3T'] = 'Position system 3 time stamp use'
	allcodes['P3Z'] = 'Position system 3 vertical location in m'
	allcodes['P3X'] = 'Position system 3 along location in m'
	allcodes['P3Y'] = 'Position system 3 athwart location in m'
	allcodes['P3D'] = 'Position system 3 time delay in seconds'
	allcodes['P3G'] = 'Position system 3 geodetic datum'
	allcodes['P3S'] = 'Position system 3 on serial line or Ethernet'
	allcodes['MSZ'] = 'Motion sensor 1 vertical location in m'
	allcodes['MSX'] = 'Motion sensor 1 along location in m'
	allcodes['MSY'] = 'Motion sensor 1 athwart location in m'
	allcodes['MRP'] = 'Motion sensor 1 roll reference plane'
	allcodes['MSD'] = 'Motion sensor 1 time delay in milliseconds'
	allcodes['MSR'] = 'Motion sensor 1 roll offset in degrees'
	allcodes['MSP'] = 'Motion sensor 1 pitch offset in degrees'
	allcodes['MSG'] = 'Motion sensor 1 heading offset in degrees'
	allcodes['NSZ'] = 'Motion sensor 2 vertical location in m'
	allcodes['NSX'] = 'Motion sensor 2 along location in m'
	allcodes['NSY'] = 'Motion sensor 2 athwart location in m'
	allcodes['NRP'] = 'Motion sensor 2 roll reference plane'
	allcodes['NSD'] = 'Motion sensor 2 time delay in milliseconds'
	allcodes['NSR'] = 'Motion sensor 2 roll offset in degrees'
	allcodes['NSP'] = 'Motion sensor 2 pitch offset in degrees'
	allcodes['NSG'] = 'Motion sensor 2 heading offset in degrees'
	allcodes['GCG'] = 'Gyrocompass heading offset in degrees'
	allcodes['MAS'] = 'Roll scaling factor'
	allcodes['SHC'] = 'Transducer depth sound speed source'
	allcodes['PPS'] = '1PPS clock synchronization'
	allcodes['CLS'] = 'Clock source'
	allcodes['CLO'] = 'Clock offset in seconds'
	allcodes['VSN'] = 'Active attitude velocity sensor'
	allcodes['VSU'] = 'Attitude velocity sensor 1 UDP port address (UDP5)'
	allcodes['VSE'] = 'Attitude velocity sensor 1 Ethernet port'
	allcodes['VTU'] = 'Attitude velocity sensor 2 UDP port address (UDP6)'
	allcodes['VTE'] = 'Attitude velocity sensor 2 Ethernet port'
	allcodes['ARO'] = 'Active roll/pitch sensor'
	allcodes['AHE'] = 'Active heave sensor'
	allcodes['AHS'] = 'Active heading sensor'
	allcodes['VSI'] = 'Ethernet 2 address'
	allcodes['VSM'] = 'Ethernet 2 IP network mask'
	allcodes['MCA'] = 'Multicast sensor IP multicast address (Ethernet 2)'
	allcodes['MCU'] = 'Multicast sensor UDP port number'
	allcodes['MCI'] = 'Multicast sensor identifier'
	allcodes['MCP'] = 'Multicast position system number'
	allcodes['SNL'] = 'Ships noise level'
	allcodes['CPR'] = 'Cartographic projection'
	allcodes['ROP'] = 'Responsible operatr'
	allcodes['SID'] = 'Survey identifier'
	allcodes['RFN'] = 'Raw File Name'
	allcodes['PLL'] = 'Survey line identifier (planned line no)'
	allcodes['COM'] = 'Comment'

	return allcodes[code]

###############################################################################
def loadARC(conditionbs):
	if not os.path.exists(conditionbs):
		print ("oops: backscatter conditioning filename does not exist, please try again: %s" % conditionbs)
		exit()
	ARCList = loadCSVFile(conditionbs)
	ARCList.pop(0)
	ARC = {}
	for item in ARCList:
		ARC[float(item[0])] = float(item[5])
	print ("Conditioning Y_SeabedImage datagrams with: %s :" % args.conditionbs)
	return ARC
		
###############################################################################
def extractProfile(datagram, TypeOfDatagram, currentRecordDateTime, latitude, longitude, filename, odir):
	'''extract the SVP profile and save it to a file for use with CARIS'''
	if (TypeOfDatagram == 'P'):
		datagram.read()
		# remember the current position, so we can use it for the SVP extraction
		latitude = datagram.Latitude
		longitude = datagram.Longitude

	if TypeOfDatagram == 'U':
		datagram.read()
		outfile = os.path.join(os.path.dirname(os.path.abspath(filename)), os.path.splitext(filename)[0] + "_SVP.svp")
		outfile = createOutputFileName(outfile)
		print("Writing SVP Profile : %s" % outfile)
		with open(outfile, 'w') as f:
			f.write("[SVP_Version_2]\n")
			f.write("%s\n" % outfile)
			
			day_of_year = (currentRecordDateTime - datetime(currentRecordDateTime.year, 1, 1)).days
			lat = decdeg2dms(latitude)
			lon = decdeg2dms(longitude)
			f.write("Section %s-%s %s:%s:%s %s:%s:%.3f %s:%s:%.3f\n" % (currentRecordDateTime.year, day_of_year, currentRecordDateTime.hour, currentRecordDateTime.minute, currentRecordDateTime.second, int(lat[0]), int(lat[1]), lat[2], int(lon[0]), int(lon[1]), lon[2] ))
			for row in datagram.data:
				f.write("%.3f %.3f \n" % (row[0], row[1]))
			f.close()
	return

###############################################################################
def extractBSCorrData(datagram, TypeOfDatagram, filename, odir):
	'''extract the BSCorr file from the Extraparameter datagram and save it to a file.  good for backscatter calibration'''
	if TypeOfDatagram == '3':
		datagram.read()
		if datagram.ContentIdentifier == 6:
			outfile = os.path.join(os.path.dirname(os.path.abspath(filename)), os.path.splitext(filename)[0] + "_BSCorr.txt")
			outfile = createOutputFileName(outfile)
			print("Writing BSCorr file : %s" % outfile)
			data = str(datagram.data).replace("\\n", "\n")
			data = data.replace("\\t", "\t")
			with open(outfile, 'w') as f:
				f.write(data)
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
def injector(outFilePtr, currentRecordTimeStamp, injectionData, counter):
	'''inject data into the output file and pop the record from the injector'''
	if len(injectionData) == 0:
		return
	recordsToAdd = []
	while ((len(injectionData) > 0) and (float(injectionData[0][0]) <= currentRecordTimeStamp)):
		recordsToAdd.append(injectionData.popleft())
	
	if len(recordsToAdd) > 0:
		# counter = counter + 1
		a = pyall.A_ATTITUDE_ENCODER()
		datagram = a.encode(recordsToAdd, counter)
		outFilePtr.write(datagram)

		# encode and inject a H_HEIGHT record containing Heave so CARIS can apply it in processing without the need to re-refract from range/bearing
		date = pyall.from_timestamp(recordsToAdd[0][0])
		recordDate = pyall.dateToKongsbergDate(date)
		recordTime = pyall.dateToKongsbergTime(date)
		h = pyall.H_HEIGHT_ENCODER()
		datagram = h.encode(recordsToAdd[0][1], recordDate, recordTime, counter)
		outFilePtr.write(datagram)
		counter = counter + 1
	return counter

# ###############################################################################
# def loadSRHFile(fileName):
#	 '''the SRH file format is the KOngsberg PFreeHeave binary file format'''
#	 with open(fileName, 'r') as f:
#		 ALLPacketHeader_fmt = '=LBBHLL'
#		 ALLPacketHeader_len = struct.calcsize(ALLPacketHeader_fmt)
#		 ALLPacketHeader_unpack = struct.Struct(ALLPacketHeader_fmt).unpack_from
#			 reader = csv.reader(f)
#			 data = list(reader)
#	 return data

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
def createOutputFileName(path, ext=""):
	'''Create a valid output filename. if the name of the file already exists the file name is auto-incremented.'''
	path = os.path.expanduser(path)

	if not os.path.exists(os.path.dirname(path)):
		os.makedirs(os.path.dirname(path))

	if not os.path.exists(path):
		return path

	if len(ext) == 0:
		root, ext = os.path.splitext(os.path.expanduser(path))
	else:
		# use the user supplied extension
		root, ext2 = os.path.splitext(os.path.expanduser(path))
	
	dir	   = os.path.dirname(root)
	fname	 = os.path.basename(root)
	candidate = fname+ext
	index	 = 1
	ls		= set(os.listdir(dir))
	while candidate in ls:
			candidate = "{}_{}{}".format(fname,index,ext)
			index	+= 1

	return os.path.join(dir, candidate)


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
	#	 return struct.unpack("<I", struct.pack(">I", i))[0]
	# def swap16(self, i):
	#	 return struct.unpack("<H", struct.pack(">H", i))[0]

# def swap32(self, x):
#		 return int.from_bytes(x.to_bytes(4, byteorder='little'), byteorder='big', signed=False)
###############################################################################
if __name__ == "__main__":
	start_time = time.time() # time  the process
	main()
	print("Duration: %d seconds" % (time.time() - start_time))
