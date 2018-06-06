#name:			pyALLConditioner
#created:		April 2018
#by:			p.kennedy@guardiangeomatics.com
#description:	python module to pre-process a Kongsberg ALL sonar file and do useful things with it
#See readme.md for more details

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
import numpy as np
# from bisect import bisect_left, bisect_right
import sortedcollection
from operator import itemgetter
from collections import deque
from collections import defaultdict
import matplotlib.pyplot as plt
from scipy import stats
from scipy import signal

###############################################################################
def main():
	parser = ArgumentParser(description='Read Kongsberg ALL file and condition the file by removing redundant records and injecting updated information to make the file self-contained.',
			epilog='Example: \n To condition a single file use -i c:/temp/myfile.all \n to condition all files in a folder use -i c:/temp/*.all\n To condition all .all files recursively in a folder, use -r -i c:/temp \n To condition all .all files recursively from the current folder, use -r -i ./ \n', formatter_class=RawTextHelpFormatter)
	parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='Search Recursively from the current folder.  [Default: False]')
	parser.add_argument('-i', dest='inputFile', action='store', help='Input ALL filename to image. It can also be a wildcard, e.g. *.all')
	parser.add_argument('-odir', dest='odir', action='store', default="", help='Specify a relative output folder e.g. -odir conditioned')
	parser.add_argument('-odix', dest='odix', action='store', default="_conditioned", help='Specify an output filename appendage e.g. -odix _savgol')
	parser.add_argument('-exclude', dest='exclude', action='store', default="", help='Exclude these datagrams.  Note: this needs to be case sensitive e.g. -x YNn')
	parser.add_argument('-injectA', dest='injectAFileName', action='store', default="", help='Inject this ATTITUDE file as "A" datagrams. e.g. -injectA "*.srh|*.txt" (Hint: remember the quotes!)')
	parser.add_argument('-injectAH', dest='injectAHFileName', action='store', default="", help='Inject this ATTIDUE+HEIGHT file as "A" and "H" datagrams. e.g. -injectAH "*.txt" (Hint: remember the quotes!)')
	parser.add_argument('-conditionbs', dest='conditionbs', action='store', default="", help='Improve the Y_SeabedImage datagrams by adding a CSV correction file. eg. -conditionbs c:/angularResponse.csv')
	parser.add_argument('-extractbs', action='store_true', default=False, dest='extractbs', help='Extract backscatter from Y datagram so we can analyse. [Default: False]')
	parser.add_argument('-nadir', action='store_true', default=False, dest='nadir', help='Output a CSV file of the nadir beam.  [Default: False]')
	parser.add_argument('-bscorr', action='store_true', default=False, dest='bscorr', help='Output the backscatter bscorr.txt file as used in the PU.  This is useful for backscatter calibration and processing, and removes the need to telnet into the PU.   [Default: False]')
	parser.add_argument('-splitd', action='store_true', default=False, dest='splitd', help='split the .all file every time the depth mode changes.  [Default: False]')
	parser.add_argument('-splitt', dest='splitt', action='store', default="", help='Split the .all file based on time inm secnds e.g. -splitt 60')
	parser.add_argument('-attitude', action='store_true', default=False, dest='attitude', help='Output a CSV file of "A" ATTITUDE.  [Default: False]')
	parser.add_argument('-attitudeHeight', action='store_true', default=False, dest='attitudeHeight', help='Output a CSV file of the COMBINED "A" ATTITUDE and "h" HEIGHT.  [Default: False]')
	parser.add_argument('-height', action='store_true', default=False, dest='height', help='Output a CSV file of "h" HEIGHT datagrams.  [Default: False]')
	parser.add_argument('-clock', action='store_true', default=False, dest='clock', help='Output a CSV file containing the clock datagrams. Very usefulfor QC of timing subsystem .  [Default: False]')
	parser.add_argument('-svp', action='store_true', default=False, dest='svp', help='Output a CARIS compatible SVP file based on the sound velocity datagram.  [Default: False]')
	parser.add_argument('-install', action='store_true', default=False, dest='install', help='Output the installation parameters to a CSV. [Default: False]')
	parser.add_argument('-runtime', dest='runtime', action='store_true', default=False, help='dump the runtime records for QC purposes')
	parser.add_argument('-wobble', dest='wobble', action='store_true', default=False, help='compute the heave and roll related wobble from the raw observations for QC purposes')
	parser.add_argument('-beamqc', dest='beamqc', action='store_true', default=False, help='for QC purposes compute a best fit line through each ping and the delta Z for each beam, then compute the mean deviation. Identify noisy beams.')

	if len(sys.argv)==1:
		parser.print_help()
		sys.exit(1)
		
	args = parser.parse_args()

	fileCounter=0
	matches				= []
	injectAttitude		= False
	injectAttitudeHeight= False
	extractAttitude		= False
	extractAttitudeHeight	= False
	extractHeight		= False
	extractNadir		= False
	extractBackscatter	= False
	extractSVP			= False
	extractClock		= False
	conditionBS			= False
	extractBSCorr		= False
	extractInstall		= False
	extractRuntime		= False
	correctBackscatter 	= False
	writeConditionedFile= True
	splitd				= False
	latitude			= 0
	longitude			= 0
	splitt				= 0
	splitfileend		= 0
	wobble				= False
	beamQC 				= False
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
	if len(args.injectAFileName) > 0:
		injectAttitude = True
		print ("Injector will inject system 1 'A' records as an active attitude data sensor with pitch,roll,heave and heading data. You may well need to also use the -exclude A to remove the existing records so the .all file is not conflicted")
		if args.injectAFileName.lower().endswith('.srh'):
			SRH = SRHReader()
			SRH.loadFiles(args.injectAFileName) # load all the filenames
			print ("Records to inject: %d" % len(SRH.SRHData))
		if args.injectAFileName.lower().endswith('.txt'):
			ATT = ATTReader()
			ATT.loadFiles(args.injectAFileName)
			print ("Records to inject: %d" % len(ATT.ATTData))

	if len(args.injectAHFileName) > 0:
		injectAttitudeHeight = True
		print ("Injector will inject system 1 'A' attitude records as an active attitude data sensor with pitch,roll,heave and heading data. You may well need to also use the -exclude A to remove the existing records so the .all file is not conflicted")
		print ("Injector will inject 'h' height records GPS height from atttitde CSV file You may well need to also use the -exclude h to remove the existing records so the .all file is not conflicted")
		if args.injectAHFileName.lower().endswith('.txt'):
			ATT = ATTReader()
			ATT.loadFiles(args.injectAHFileName)
			print ("Records to inject: %d" % len(ATT.ATTData))
		# auto exclude attitude records.  on reflection, we should probably NOT do this.
		# args.exclude = 'n'

	if args.install:
		extractInstall=True
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

	if args.runtime:
		extractRuntime=True
		writeConditionedFile= False #we do not need to write out a .all file
		# find the first record so we can print the header from the datagram class
		r = pyall.ALLReader(matches[0])
		while r.moreData():
			typeOfDatagram, datagram = r.readDatagram()
			if typeOfDatagram == 'R':
				datagram.read()
				print("filename," + datagram.header())
				break
		r.close()

	if args.clock:
		extractClock=True
		timestamps=[]
		writeConditionedFile= False #we do not need to write out a .all file
		print("RecordDate,ExternalDate,RecordTime,ExternalTime,Difference,PPSInUse")

	if args.attitude:
		extractAttitude=True
		writeConditionedFile= False #we do not need to write out a .all file

	if args.height:
		extractHeight=True
		writeConditionedFile= False #we do not need to write out a .all file

	if args.attitudeHeight:
		extractAttitudeHeight=True
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
	
	if args.beamqc:
		beamQC=True
		heads = {}
		writeConditionedFile= False
		r = pyall.ALLReader(matches[0])
		# we need the head installation parameters so we can compute and use the take off angles.
		installStart, installStop, initialMode, datagram = r.loadInstallationRecords()
		head = getHead(heads, datagram.SerialNumber)
		head.installationParameters = datagram.installationParameters
		head.Roll = head.installationParameters['S1R']
		head = getHead(heads, datagram.SecondarySerialNumber)
		head.installationParameters = datagram.installationParameters
		head.Roll = head.installationParameters['S2R']
		r.close()

	if args.wobble:
		wobble=True
		writeConditionedFile= False
		wobbleResults = []
		attitudeData = []
# #################################################################################
	for filename in matches:
		if extractAttitude:
			# create an output file based on the input
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName = os.path.splitext(outFileName)[0]+'_ATTITUDE.txt'
			outFileName  = addFileNameAppendage(outFileName, args.odix)
			outFileName  = createOutputFileName(outFileName)
			outAttitudeFilePtr = open(outFileName, 'w')
			print ("writing ATTITUDE to file: %s" % outFileName)

		if extractHeight:
			# create an output file based on the input
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName = os.path.splitext(outFileName)[0]+'_HEIGHT.txt'
			outFileName  = addFileNameAppendage(outFileName, args.odix)
			outFileName  = createOutputFileName(outFileName)
			outHeightFilePtr = open(outFileName, 'w')
			print ("writing HEIGHT to file: %s" % outFileName)

		if extractAttitudeHeight:
			# create an output file based on the input
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName = os.path.splitext(outFileName)[0]+'_ATTITUDEHEIGHT.txt'
			outFileName  = addFileNameAppendage(outFileName, args.odix)
			outFileName  = createOutputFileName(outFileName)
			outAttitudeHeightFilePtr = open(outFileName, 'w')
			print ("writing ATTITUDE+HEIGHT to file: %s" % outFileName)
			attitudeData = []
			heightData = []

		if writeConditionedFile:
			# create an output file based on the input
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName  = addFileNameAppendage(outFileName, args.odix)
			outFileName  = createOutputFileName(outFileName)
			outFilePtr = open(outFileName, 'wb')
			print ("writing to file: %s" % outFileName)

		# open the file and do some initialisation stuff 
		r = pyall.ALLReader(filename)
		counter = 0

		if extractNadir:
			print ("NadirDepth,TxDepth,Roll,Pitch,Heave,Heading ")
		
		if extractAttitude:
			# read the first record so we get a date for the file header
			typeOfDatagram, datagram = r.readDatagram()
			str = r.currentRecordDateTime().strftime('%Y%m%d') + ",Timestamp, Roll, Pitch, Heave, Heading"
			outAttitudeFilePtr.write("Name:" + os.path.basename(filename) + "," + str + "\n")

		if extractHeight:
			# read the first record so we get a date for the file header
			typeOfDatagram, datagram = r.readDatagram()
			str = r.currentRecordDateTime().strftime('%Y%m%d') + ",Timestamp, Height"
			outHeightFilePtr.write("Name:" + os.path.basename(filename) + "," + str + "\n")

		if extractAttitudeHeight:
			# read the first record so we get a date for the file header
			typeOfDatagram, datagram = r.readDatagram()
			str = r.currentRecordDateTime().strftime('%Y%m%d') + ",Timestamp, Roll, Pitch, Heave, Heading, Height"
			outAttitudeHeightFilePtr.write("Name:" + os.path.basename(filename) + "," + str + "\n")

		if splitd:
			InstallStart, InstallEnd, initialDepthMode = r.loadInstallationRecords()
		if splitt > 0:
			InstallStart, InstallEnd, initialDepthMode = r.loadInstallationRecords()

		if injectAttitude:					
			TypeOfDatagram, datagram = r.readDatagram()
			if args.injectAFileName.lower().endswith('.srh'):
				# kill off the leading records so we do not swamp the filewith unwanted records
				SRHSubset = deque(SRH.SRHData)
				SRHSubset = trimInjectionData(pyall.to_timestamp(r.currentRecordDateTime()), SRHSubset)
				r.rewind()
			if args.injectAFileName.lower().endswith('.txt'):
				# kill off the leading records so we do not swamp the filewith unwanted records
				ATTSubset = deque(ATT.ATTData)
				ATTSubset = trimInjectionData(pyall.to_timestamp(r.currentRecordDateTime()), ATTSubset)
				r.rewind()
		if injectAttitudeHeight:					
			TypeOfDatagram, datagram = r.readDatagram()
			if args.injectAHFileName.lower().endswith('.txt'):
				# kill off the leading records so we do not swamp the filewith unwanted records
				ATTSubset = deque(ATT.ATTData)
				ATTSubset = trimInjectionData(pyall.to_timestamp(r.currentRecordDateTime()), ATTSubset)
				r.rewind()
				lastHeightTimeStamp = 0
		if extractSVP:
			# we need the position of the SVP dip in the SVP file, so use the first position record in the file
			nav = r.loadNavigation(True)
			if len(nav) > 0:
				latitude = nav[0][1]
				longitude = nav[0][2]

		currPitch = 0
		currRoll = 0
		currHeave = 0
		currHeading = 0
		currRuntime = ""
		###############################################################
		################ main loop through all records ################
		###############################################################
		while r.moreData():
			# read a datagram.  If we support it, return the datagram type and aclass for that datagram
			TypeOfDatagram, datagram = r.readDatagram()

			if splitfileend == 0:
				splitfileend = pyall.to_timestamp(r.currentRecordDateTime())

			# read the bytes into a buffer 
			rawBytes = r.readDatagramBytes(datagram.offset, datagram.numberOfBytes)

			if beamQC:
				if TypeOfDatagram == 'f':
					datagram.read()
				if TypeOfDatagram == 'D' or TypeOfDatagram == 'X':
					datagram.read()
					if len(datagram.AcrossTrackDistance) > 1:
						# figure out which head
						head = getHead(heads, datagram.SerialNumber)

						# compute a best fit line through the ping of data so we can compute the slope and intercept
						slope, intercept, rvalue, pvalue, stderr = stats.linregress(datagram.AcrossTrackDistance, datagram.Depth)
						# y = mx + c
						for idx, val in enumerate(datagram.AcrossTrackDistance):
							# if datagram.QualityFactor[idx] > 0:
							# 	continue #skip rejected beams
							if datagram.BeamDepressionAngle[idx] < 30:
								continue
							if not datagram.BeamNumber[idx] in head.beamSum:
								head.beamSum[datagram.BeamNumber[idx]] = (datagram.Depth[idx] - ((slope * val) + intercept))
								head.beamAngle[datagram.BeamNumber[idx]] = datagram.BeamDepressionAngle[idx]
								head.beamCount[datagram.BeamNumber[idx]] = 1
							else:
								head.beamSum[datagram.BeamNumber[idx]] += (datagram.Depth[idx] - ((slope * val) + intercept))
								head.beamAngle[datagram.BeamNumber[idx]] = datagram.BeamDepressionAngle[idx]
								head.beamCount[datagram.BeamNumber[idx]] += 1

			if wobble:
				if TypeOfDatagram == 'D' or TypeOfDatagram == 'X':
					datagram.read()
					# intercept is hwobble
					# slope is rwobble
					if len(datagram.AcrossTrackDistance) > 1:
						# compute a best fit line through the ping of data so we can compute the slope and intercept
						slope, intercept, rvalue, pvalue, stderr = stats.linregress(datagram.AcrossTrackDistance, datagram.Depth)
						wobbleResults.append([pyall.to_timestamp(r.currentRecordDateTime()), intercept, slope, stderr])
					else:
						print (len(datagram.AcrossTrackDistance), len(datagram.Depth))

				if TypeOfDatagram == 'A':
					datagram.read()
					for a in datagram.Attitude:
						dateobject = pyall.to_DateTime(a[0], a[1])
						# date, time, roll, pitch, heave, heading
						str = ("%d,%.3f,%.3f,%.3f,%.3f,%.3f\n" % (a[0],a[1],a[3],a[4],a[5],a[6]))
						ts = pyall.to_timestamp(pyall.to_DateTime(a[0], a[1]))
						attitudeData.append([ts, a[3], a[4], a[5], a[6]])

			if extractNadir:
				if TypeOfDatagram == 'D' or  TypeOfDatagram == 'X':
					datagram.read()
					# find the depth nearest to Nadir
					# https://stackoverflow.com/questions/9706041/finding-index-of-an-item-closest-to-the-value-in-a-list-thats-not-entirely-sort
					nadirBeam = min(range(len(datagram.AcrossTrackDistance)), key=lambda i: abs(datagram.AcrossTrackDistance[i]))
					print ("%.3f,%.3f,%.3f,%.3f,%.3f" % (datagram.Depth[nadirBeam], datagram.TransducerDepth/100, currRoll, currPitch, currHeave, currHeading))
					
				# if TypeOfDatagram == 'A':
				# 	datagram.read()
				# 	currRoll = datagram.Attitude[-1][3]
				# 	currPitch = datagram.Attitude[-1][4]
				# 	currHeave = datagram.Attitude[-1][5]
				# 	currHeading = datagram.Attitude[-1][6]

			if extractClock:
				if TypeOfDatagram == 'C':
					datagram.read()
					print (datagram)
					timestamps.append(datagram.time-datagram.ExternalTime)
			if extractAttitude:
				if TypeOfDatagram == 'A':
					datagram.read()
					for a in datagram.Attitude:
						ts = pyall.to_timestamp(pyall.to_DateTime(a[0], a[1])) #remember to add the millisecs for each sub record!
						# timetamp, roll, pitch, heave, heading
						str = ("%.3f,%.3f,%.3f,%.3f,%.3f\n" % (ts,a[3],a[4],a[5],a[6]))
						outAttitudeFilePtr.write(str)

			if extractHeight:
				if TypeOfDatagram == 'h':
					datagram.read()
					# dateobject = pyall.to_DateTime(a[0], a[1])
					# date, time, height
					ts = pyall.to_timestamp(pyall.to_DateTime(datagram.RecordDate, datagram.Time))
					str = ("%.3f,%.3f\n" % (ts, datagram.Height))
					# str = ("%d,%.3f,%.3f,%.3f,%.3f,%.3f\n" % (datagram.RecordDate, datagram.Time, datagram.Height, datagram.Height, datagram.Height, datagram.Height))
					outHeightFilePtr.write(str)

			if extractAttitudeHeight:
				if TypeOfDatagram == 'A':
					datagram.read()
					for a in datagram.Attitude:
						# dateobject = pyall.to_DateTime(a[0], a[1])
						# date, time, roll, pitch, heave, heading
						ts = pyall.to_timestamp(pyall.to_DateTime(a[0], a[1]))  #remember to add the millisecs for each sub record!
						attitudeData.append([ts, a[3], a[4], a[5], a[6]])
				if TypeOfDatagram == 'h':
					datagram.read()
					ts = pyall.to_timestamp(pyall.to_DateTime(datagram.RecordDate, datagram.Time))
					heightData.append([ts, datagram.Height])

			if extractInstall:
				if TypeOfDatagram == 'I':
					datagram.read()
					row = filename
					for i in datagram.installationParameters :
						if len(datagram.installationParameters[i]) == 0:
							datagram.installationParameters[i] = "0.00"
						row = row + "," + datagram.installationParameters[i]
						# row.replace(",,",",")
					print (row)
					# break
			if extractRuntime:
				if TypeOfDatagram == 'R':
					datagram.read()
					if currRuntime != datagram.parameters():
						print (filename, "," ,datagram)
						currRuntime = datagram.parameters()
					# break
			if splitt:
					# datagram.read()
					if pyall.to_timestamp(r.currentRecordDateTime()) > (splitfileend + splitt):
						# write out the closing install record then close the file
						print ("closing the file as the duration has exceeded the split time")
						outFilePtr.write(InstallEnd)
						outFilePtr.close()
						
						outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
						outFileName  = addFileNameAppendage(outFileName, args.odix)
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
						outFileName  = addFileNameAppendage(outFileName, args.odix)
						outFileName  = createOutputFileName(outFileName)
						outFilePtr = open(outFileName, 'wb')
						print ("writing to split file: %s" % outFileName)
						outFilePtr.write(InstallStart)
						outFilePtr.write(rawBytes)
						initialDepthMode = datagram.DepthMode #remember the new depth mode!
						
			# before we write the datagram out, we need to inject records with a smaller from_timestamp
			if injectAttitude:					
				if TypeOfDatagram in args.exclude:
					# dont trigger on records we are rejecting!		
					continue
				if args.injectAFileName.lower().endswith('.srh'):
					counter, lastHeightTimeStamp = injector(outFilePtr, pyall.to_timestamp(r.currentRecordDateTime()), SRHSubset, counter)
				if args.injectAFileName.lower().endswith('.txt'):
					counter, lastHeightTimeStamp = injector(outFilePtr, pyall.to_timestamp(r.currentRecordDateTime()), ATTSubset, counter)

			if injectAttitudeHeight:					
				if TypeOfDatagram in args.exclude:
					# dont trigger on records we are rejecting!		
					continue
				if args.injectAHFileName.lower().endswith('.txt'):
					counter, lastHeightTimeStamp = injector(outFilePtr, pyall.to_timestamp(r.currentRecordDateTime()), ATTSubset, counter, True, lastHeightTimeStamp)

				# this is a testbed until we figure out how caris handles the application of heave.
				# if TypeOfDatagram == 'X':
				#	 datagram.read()
				#	 # now encode the datagram back, making changes along the way
				#	 datagram.TransducerDepth = 999
				#	 dg = datagram.encode()
				#	 outAttitudeFilePtr.write(dg)
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

			if r.recordCounter > 1000:
				break
		# update_progress("Processed: %s (%d/%d)" % (filename, fileCounter, len(matches)), (fileCounter/len(matches)))
		fileCounter +=1
		r.close()

		if extractAttitudeHeight:
			# now we need to merge the heights into the attitude records using a time interpolation
			ts_height = cTimeSeries(heightData)
			for rec in attitudeData:
				height = ts_height.getValueAt(rec[0])
				# rec.append(height)
				# now save to the regular file format...
				# d = pyall.dateToKongsbergDate(from_timestamp(rec[0]))
				# t = pyall.dateToKongsbergTime(from_timestamp(rec[0]))
				# str = ("%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f\n" % ( d, t, rec[1], rec[2], rec[3], rec[4], height))
				str = ("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n" % (rec[0], rec[1], rec[2], rec[3], rec[4], height))
				outAttitudeHeightFilePtr.write(str)
			
		if writeConditionedFile:
			print ("Saving conditioned file to: %s" % outFileName)		
			outFilePtr.close()

		if extractClock:
			plt.figure(figsize=(12,4))
			# plt.axhline(0, color='black', linewidth=0.3)
			plt.grid(linestyle='-', linewidth='0.2', color='black')

			raw = plt.plot(timestamps, color='red', linewidth=0.5, label='Clock Difference')

			plt.legend()
			plt.xlabel('Sample #')
			plt.ylabel('Record - External Clock Difference(Sec)')
			plt.title("Clock Stability:" + os.path.basename(filename))
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName  = createOutputFileName(outFileName)
			plt.savefig(os.path.splitext(outFileName)[0]+'_clock.png', dpi = 300)
			plt.close()
			timestamps.clear()

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
		if wobble:
			plt.figure(figsize=(12,4))
			# plt.axhline(0, color='black', linewidth=0.3)
			plt.grid(linestyle='-', linewidth='0.2', color='black')

			# extract the lists of data for display
			w = np.array(wobbleResults)
			tWobble = w[:,0]
			hWobble = w[:,1]
			rWobble = w[:,2]			
			raw = plt.plot(tWobble, rWobble, color='red', linewidth=0.5, label='RWobble')
			# plot the HWobble moving it nearer to the zero origin
			raw = plt.plot(tWobble, hWobble - np.average(hWobble), 'ro', label='Levelled HWobble')
			hWobble = hWobble - np.average(hWobble)

			# isolate the low frequency signal in the heave(which should not exist)
			level = 10
			sm_hWobble = signal.savgol_filter(hWobble, 11, 1) 
			# # for i in range(level):
			# # 	smoothedHeave = signal.savgol_filter(smoothedHeave, 11, 1) 
			# # subtract the very smoothed signal from the input signal, thereby applying a lowcut filter (AKA high band pass)
			# diff = np.subtract(hWobble, smoothedHeave)

			# raw = plt.plot(tWobble, hWobble, color='blue', linewidth=0.5, label='HWobble')
			# raw = plt.plot(tWobble, sm_hWobble, color='gray', linewidth=1.5, label='SmoothedHeave')


			d = np.array(attitudeData)
			tAttitude = d[:,0]
			roll = d[:,1] / 10
			roll = roll - np.average(roll)

			pitch = d[:,2] / 10
			pitch = pitch - np.average(pitch)

			heave = d[:,3]
			# heave = heave - np.average(heave)

			raw = plt.plot(tAttitude, roll, color='yellow', linewidth=1, label='Roll')
			raw = plt.plot(tAttitude, pitch, color='blue', linewidth=1, label='Pitch')
			# raw = plt.plot(tAttitude, heave, 'bo', label='Heave')
			# raw = plt.plot(tAttitude, heave, color='green', linewidth=1, label='Heave')

			#######################
			# savgol the raw heave
			# subtract the settled heave
			# then plot and correlate to pitch/roll
			level = 1000
			sm_heave = signal.savgol_filter(heave, 101, 1) 
			for i in range(level):
				sm_heave = signal.savgol_filter(sm_heave, 101, 1) 
			# subtract the very smoothed signal from the input signal, thereby applying a lowcut filter (AKA high band pass)
			settledHeave = np.subtract(heave, sm_heave)
			# raw = plt.plot(tAttitude, settledHeave, color='black', linewidth=2, label='Heave')
			ts_heave = cTimeSeries(tAttitude, settledHeave)
			corr_hWobble = []
			for t, h in zip(tWobble, hWobble):
				correction = ts_heave.getValueAt(t)
				corr_hWobble.append(h + correction)
			raw = plt.plot(tWobble, corr_hWobble, color='black', linewidth=1, label='HeaveCorrectedNadirDepth')
			#######################

			plt.legend()
			plt.xlabel('Sample #')
			plt.ylabel('Wobble')
			plt.title("Wobble Errors:" + os.path.basename(filename))
			outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
			outFileName  = addFileNameAppendage(outFileName, args.odix)
			outFileName  = createOutputFileName(outFileName)
			plt.show()
			plt.savefig(os.path.splitext(outFileName)[0]+'_wobble.png', dpi = 300)
			plt.close()
			# timestamps.clear()
	
	if beamQC:
		plt.figure(figsize=(12,4))
		# plt.axhline(0, color='black', linewidth=0.3)
		plt.grid(linestyle='-', linewidth='0.2', color='black')

		trace=[]
		names =[]
		for key, head in heads.items():
			names.append(head.ID)
			sum = head.beamSum.values()
			count = head.beamCount.values()
			beam = head.beamCount.keys()
			profile = []
			for s,c in zip(sum, count):
				profile.append(s/c)

			trace.append(plt.bar(beam, profile))

		plt.legend([trace[0], trace[1]], names)
		plt.xlabel('Beam #')
		plt.ylabel('Deviation(m)')
		plt.title("Mean Slope Rectified Profile")
		plt.ylim(-0.1,0.1)

		# plt.text(0.05, 0.95, heads[names[0]].beamCount[100], fontsize=14, verticalalignment='top')


		outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
		outFileName  = createOutputFileName(outFileName)
		plt.savefig(os.path.splitext(outFileName)[0]+'_BeamQC.png', dpi = 300)
		plt.show()
		plt.close()
	
	# update_progress("Process Complete: ", (fileCounter/len(matches)))

def getHead(heads, serialNumber):
	if not serialNumber in heads:
		heads[serialNumber] = cMBESHead(serialNumber)
	return heads[serialNumber]

###############################################################################
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
		# outFileName  = addFileNameAppendage(outFileName, args.odix)
		outfile = createOutputFileName(outfile)
		print("Writing SVP Profile : %s" % outfile)
		with open(outfile, 'w') as f:
			f.write("[SVP_Version_2]\n")
			f.write("%s\n" % outfile)
			
			day_of_year = (currentRecordDateTime - datetime(currentRecordDateTime.year, 1, 1)).days
			lat = decdeg2dms(latitude)
			lon = decdeg2dms(longitude)
			f.write("Section %s-%s %s:%s:%s %s:%s:%.3f %s:%s:%.3f\n" % (currentRecordDateTime.year, day_of_year, currentRecordDateTime.strftime("%H"), currentRecordDateTime.strftime("%M"), currentRecordDateTime.strftime("%S"), int(lat[0]), int(lat[1]), lat[2], int(lon[0]), int(lon[1]), lon[2] ))
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
def injector(outFilePtr, currentRecordTimeStamp, injectionData, counter, injectHeight=False, lastHeightTimeStamp=0):
	'''inject data into the output file and pop the record from the injector'''
	if len(injectionData) == 0:
		return counter, lastHeightTimeStamp
	recordsToAdd = []
	while ((len(injectionData) > 0) and (float(injectionData[0][0]) <= currentRecordTimeStamp)):
		# limit the data to be injected to within 2 seconds of the current record.  If not we can get thousands of records at start of file.
		if (currentRecordTimeStamp - float(injectionData[0][0])) < 2:
			recordsToAdd.append(injectionData.popleft())
		else:
			injectionData.popleft()
	
	if len(recordsToAdd) > 0:
		# counter = counter + 1
		a = pyall.A_ATTITUDE_ENCODER()
		datagram = a.encode(recordsToAdd, counter)
		outFilePtr.write(datagram)

		if injectHeight:
			# lastTimeStamp = 0
			h = pyall.h_HEIGHT_ENCODER()
			for rec in recordsToAdd: 
				ts = float(rec[0])
				if (ts - lastHeightTimeStamp) > 1.0: #only write 1 height record per 1 seconds second to save space.
					firstRecordDate = from_timestamp(ts)
					recordDate = int(pyall.dateToKongsbergDate(firstRecordDate))
					recordTime = int(pyall.dateToSecondsSinceMidnight(firstRecordDate)*1000)
					height = float(rec[5])

					datagram = h.encode(height, recordDate, recordTime, counter)
					outFilePtr.write(datagram)
					counter = counter + 1
					lastHeightTimeStamp = ts
	# resent the counter so it never overflowa the 16 bit number in the .all datagram field
	if counter > 65536:
		counter = 0
	return counter, lastHeightTimeStamp

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
def addFileNameAppendage(path, appendage):
	'''Create a valid output filename. if the name of the file already exists the file name is auto-incremented.'''
	path = os.path.expanduser(path)

	if not os.path.exists(os.path.dirname(path)):
		os.makedirs(os.path.dirname(path))

	# if not os.path.exists(path):
	# 	return path

	root, ext = os.path.splitext(os.path.expanduser(path))
	dir	   = os.path.dirname(root)
	fname	 = os.path.basename(root)
	candidate = "{}{}{}".format(fname, appendage, ext)

	return os.path.join(dir, candidate)

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
	
	dir		= os.path.dirname(root)
	fname	= os.path.basename(root)
	candidate = fname+ext
	index	= 1
	ls		= set(os.listdir(dir))
	while candidate in ls:
			candidate = "{}_{}{}".format(fname,index,ext)
			index	+= 1

	return os.path.join(dir, candidate)

###############################################################################
class cMBESHead:
	def __init__(self, ID):
		self.ID = ID
		self.beamSum = {}
		self.beamCount = {}
		self.beamAngle = {}
		self.installationParameters = ''
		self.Roll = 0

###############################################################################
class cWobble:
	def __init__(self):
		self.time = []
		self.hwobble = []
		self.rwobble = []
		self.stderr = []

	def add(self, time, hwobble, rwobble, stderr):
		self.time = time
		self.hwobble = hwobble
		self.rwobble = rwobble
		self.stderr = stderr

###############################################################################
class cTimeSeries:
	'''# how to use the time series class, a 2D list of time
	# attitude = [[1,100],[2,200], [5,500], [10,1000]]
	# tsRoll = cTimeSeries(attitude)
	# print(tsRoll.getValueAt(6))'''
	# def __init__(self, list2D):
	# 	'''the time series requires a 2d series of [[timestamp, value],[timestamp, value]].  It then converts this into a numpy array ready for fast interpolation'''
	# 	self.name = "2D time series"

	def __init__(self, timeOrTimeValue, values=""):
		'''the time series requires a 2d series of [[timestamp, value],[timestamp, value]].  It then converts this into a numpy array ready for fast interpolation'''
		self.name = "2D time series"
		if len(values) == 0:
				arr = np.array(timeOrTimeValue)
				self.times = arr[:,0]
				self.values = arr[:,1]
		else:
			self.times = np.array(timeOrTimeValue)
			self.values = np.array(values)

	def getValueAt(self, timestamp):
		return np.interp(timestamp, self.times, self.values, left=None, right=None)

###############################################################################
class ATTReader:
	'''class to read a Guardian Attitude file'''
	'''This class may need to read multiple txt files, merge them, sort and provide rapid access using the bisect tools in python'''
	def __init__(self):
		self.ATTData = deque()

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
		print ("Loading Attitude Files:")
		for f in matches:
			self.loadfile(f)
		return

	def loadfile(self, filename):
		if not os.path.isfile(filename):
			print ("Attitude file not found:", filename)
			return
		print (filename)
		with open(filename, 'r') as csvfile:
				reader = csv.reader(csvfile, delimiter=',', quotechar='|')
				header = next(reader) 
				for row in reader:
					# convert the caris generic data parser format into a regular timestamp
					# d = pyall.to_DateTime(row[0], float(row[1]))
					# timestamp = pyall.to_timestamp(d)
					timestamp = float(row[0])
					roll	= float(row[1])
					pitch	= float(row[2])
					heave	= float(row[3])
					heading = float(row[4])
					if len(row) > 4:
						height = float(row[5])
						self.ATTData.append([timestamp, roll, pitch, heave, heading, height])
					else:
						self.ATTData.append([timestamp, roll, pitch, heave, heading, 0.000])

					# roll	= float(row[2])
					# pitch	= float(row[3])
					# heave	= float(row[4])
					# heading = float(row[5])
					# self.ATTData.append([timestamp, roll, pitch, heave, heading])

		# fileptr = open(filename, 'r')		
		# fileSize = os.path.getsize(filename)
		# try:
		# 	while True:
		# 		data = fileptr.read(self.SRHPacket_len)
		# 		if not data: break
		# 		s = self.SRHPacket_unpack(data)
		# 		timestamp = float(s[3]) + (s[4] * 0.0001)
		# 		heave = float(s[5]) * 0.01
		# 		self.SRHData.append([timestamp, heave])
		# 		# print (from_timestamp(timestamp), heave)
		# except struct.error:
		# 	print ("Exception loading Attitude file.  Will process as much as can be read")

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
		roll=0.0
		pitch=0.0
		heading=0.0
		try:
			while True:
				data = fileptr.read(self.SRHPacket_len)
				if not data: break
				s = self.SRHPacket_unpack(data)
				timestamp = float(s[3]) + (s[4] * 0.0001)
				heave = float(s[5]) * 0.01 #heave in metres
				self.SRHData.append([timestamp, pitch, roll, heave, heading]) #for consistency send all attitude values even if they are empty
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
