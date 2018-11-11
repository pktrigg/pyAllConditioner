# POSMVRead.py
# =====
# created:		  May 2018
# version		   1.0.
# by:			   paul.kennedy@guardiangeomatics.com
# description:	  python module to read an Applanix .000 binary file
# notes:			See main at end of script for example how to use this
# based on ICD file version 4  
# developed for Python version 3.4 
# See readme.md for more details

import sys
from glob import glob
import ctypes
import argparse
import math
import pprint
import struct
import os.path
import time
import re
from datetime import datetime
from datetime import timedelta
import datetime, calendar
import numpy as np
import matplotlib.pyplot as plt

###############################################################################
def main():
	parser = argparse.ArgumentParser(description='Read POSMV file and convert into human readable format.')
	parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='Search Recursively from the current folder.  [Default: False]')
	parser.add_argument('-i', dest='inputFile', action='store', help='Input ALL filename to image. It can also be a wildcard, e.g. *.all')
	parser.add_argument('-s', dest='step', action='store', type=float, default=0, help='step through the records and sample every n seconds, e.g. -s 10')
	parser.add_argument('-odir', dest='odir', action='store', default="", help='Specify a relative output folder e.g. -odir conditioned')
	parser.add_argument('-odix', dest='odix', action='store', default="_savgol", help='Specify an output filename appendage e.g. -odix _savgol')
	parser.add_argument('-summary', dest='summary', action='store_true', default=False, help='dump a summary of the records in the file')
	parser.add_argument('-installation', dest='installation', action='store_true', default=False, help='dump the install records in the file so we can QC changes')
	parser.add_argument('-heave', dest='heave', action='store_true', default=False, help='dump the TRUE HEAVE from group 111 at full rate')
	parser.add_argument('-position', dest='position', action='store_true', default=False, help='dump the POSITION from group 1 at full recorded rate (1Hz)')
	parser.add_argument('-attitude', dest='attitude', action='store_true', default=False, help='dump the ATTITUDE from group 4 at full recorded rate (1Hz)')
	parser.add_argument('-warning', dest='warning', action='store', default="", help='dump the user requested warnings from group 10 messages, e.g. -warning GPS to dumpy messages containing the string GPS.  for everything, use -warning ,  for errors use -warning ** or -warning error')
	parser.add_argument('-v', dest='verbose', action='store_true', default=False, help='dump with verbosity (1Hz)')

	if len(sys.argv)==1:
		parser.print_help()
		sys.exit(1)
	
	args = parser.parse_args()

	matches				= []

	if args.recursive:
		for root, dirnames, filenames in os.walk(os.path.dirname(args.inputFile)):
			for f in fnmatch.filter(filenames, '*.all'):
				matches.append(os.path.join(root, f))
	else:
		if os.path.exists(args.inputFile):
			matches.append (os.path.abspath(args.inputFile))
		else:
			for filename in glob(args.inputFile):
				matches.append(filename)

	# trim out the folders from the filespec
	m = []
	for filename in matches:
		if os.path.isdir(filename):
			print ("this is a folder, skipping...", filename)
		else:
			m.append(filename)
	matches = m		

	if len(matches) == 0:
		print ("Nothing found in %s to condition, quitting" % args.inputFile)
		exit()

	if args.summary:
		print ("Scanning file to count all records...")
	
	if args.installation:
		totalRecords = 0

	if args.heave:
		totalRecords = 0
		heaveData = []

# #################################################################################
	#open the file for reading by creating a new POSReader class and passin in the filename to open.
	for filename in matches:

				# create an output file based on the input
		outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
		outFileName  = addFileNameAppendage(outFileName, args.odix)
		outFileName  = createOutputFileName(outFileName)
		# outFilePtr = open(outFileName, 'wb')
		# print ("writing to file: %s" % outFileName)

		start_time = time.time() # time the process
		first, last = getFirstLastTimeStamps(filename)
		print ("Duration %.3f First %s Last %s" % (time.time() - start_time, str(from_timestamp(first)), str(from_timestamp(last)))) 

		summary = {}
		r = POSReader(filename)
		r.findGPSWeek()
		# start_time = time.time() # time the process
		lastrecordTimeStamp = to_timestamp(r.fileStartDateObject)
		lastMsg = "" # reduce the output of duplicate strings.

		if args.summary:
			print ("First Record Time:", r.fileStartDateObject)
			startDate = r.fileStartDateObject
		print ("Processing:" + filename)
		while r.moreData():
			# read a datagram.  If we support it, return the datagram type and a class for that datagram
			# The user then needs to cPOS the read() method for the class to undertake a fileread and binary decode.  This keeps the read super quick.
			groupID, datagram = r.readDatagram()
			if groupID==False:
				print ("invalid file format.This is not a POSMv File")
				break
			
			# if (groupID == 29): # marinestar status
			# 	datagram.read()

			if args.installation:
				if (groupID == 20): # GeneralInstallation
					datagram.read()
					if totalRecords== 0:
						print ("Date, Filename," + datagram.header())
					totalRecords += 1
					msg = str(datagram)
					if msg == lastMsg:
						continue
					# the installation has changed, so print it
					print (str(from_timestamp(lastrecordTimeStamp)) +", " + filename + ", " + msg)
					lastrecordTimeStamp = r.recordTimeStamp
					lastMsg = msg
					continue
			
			if args.warning:
				if (groupID == 10): # "MSG General Status & FDIR - ERROR MESSAGES!"
					if (r.recordTimeStamp - lastrecordTimeStamp) > args.step:
						datagram.read()
						if args.warning in str(datagram):
							print (datagram)
							lastrecordTimeStamp = r.recordTimeStamp
					continue

			if args.position:
				if (groupID == 1): 
					datagram.read()
					if totalRecords == 0:
						print (datagram.header())
					totalRecords += 1

					print(datagram)
					continue

			if args.heave:
				if (groupID == 111):
						datagram.read()
						heaveData.append([datagram.timeStamp, datagram.heave, datagram.heaveTime1, datagram.trueHeave]) 
						# if (r.recordTimeStamp - lastrecordTimeStamp) > args.step:						
						# 	if totalRecords == 0:
						# 		print (datagram.header())
						# 	else:
						# 		print (datagram)						
						# 		lastrecordTimeStamp = r.recordTimeStamp
						totalRecords += 1
						continue

			if args.attitude:
				if (groupID == 4): 
					datagram.read()
					# if totalRecords== 0:
					# 	print (datagram.header())
					totalRecords += 1
					print(datagram)
					continue

			# if (groupID == 112): # "NMEA Strings"
			# 	datagram.read()

			if not groupID in summary:
				summary[groupID] = 1
			else:
				summary[groupID] += 1

		if args.summary:
			totalRecords = 0
			print ("Last Record Date:", from_timestamp(r.timeOrigin + r.recordTimeStamp))
			print ("TotalRecords, GroupName")
			for k,v in sorted(summary.items()):
				totalRecords += v
				print ("%7d, %s" %(v, getDatagramName(k)))
			print ("%10d, %s" % (totalRecords, "Total Records"))
			print ("File Duration:" , 		from_timestamp(r.timeOrigin + r.recordTimeStamp) - startDate)

		if args.heave:
			createPlots(filename, outFileName, heaveData)

###############################################################################
def loadHeaveBetweenTimesStamps(filename, first, last):
	heaveData = []
	r = POSReader(filename)
	r.findGPSWeek()
	r.rewind()
	# firstRecordTimeStamp = to_timestamp(r.fileStartDateObject)
	while r.moreData():
		groupID, datagram = r.readDatagram()
		if r.recordTimeStamp < first:
			continue

		if (groupID == 111):
			datagram.read()
			heaveData.append([datagram.timeStamp, datagram.heave, datagram.heaveTime1, datagram.trueHeave]) 
			# we have all the data we can possibly need from this file so quit.
			if datagram.timeStamp > last:
				break

	r.close()
	return heaveData

###############################################################################
def getFirstLastTimeStamps(filename, startTimeStamp=0):
	r = POSReader(filename)
	r.findGPSWeek()
	firstRecordTimeStamp = to_timestamp(r.fileStartDateObject)
	# to speed things up, if a start time is passed in, we can determine if the file is within a day, and if not, quit without reading.  This makes it much faster
	if startTimeStamp > 0:
		if  startTimeStamp - 86400 <= firstRecordTimeStamp <= startTimeStamp + 86400:
			while r.moreData():
				groupID, datagram = r.readDatagram()
			lastRecordTimeStamp = r.recordTimeStamp
		else:
			lastRecordTimeStamp = 0
	r.close()
	return firstRecordTimeStamp,lastRecordTimeStamp

###############################################################################
def createPlots(filename, outFileName, attitudeData):
		arr = np.array(attitudeData)
		ts = arr[:,0]
		heave = arr[:,1]
		tsth = arr[:,2]
		trueHeave = arr[:,3]
		
		plt.figure(figsize=(12,4))
		# plt.axhline(0, color='black', linewidth=0.3)
		plt.grid(linestyle='-', linewidth='0.2', color='black')

		raw = plt.plot(ts, heave, color='red', linewidth=0.2, label='RealTime Heave')
		error = plt.plot(tsth, trueHeave, color='green', linewidth=0.5, label='True Heave')

		plt.legend()
		plt.xlabel('Sample #')
		plt.ylabel('Heave(m)')
		plt.title(os.path.basename(filename))
		plt.savefig(os.path.splitext(outFileName)[0]+'.png', dpi = 300)
		plt.show()

###############################################################################
class C_M56: 
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.name = "General Data Message"
		self.typeOfDatagram = 56
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin
		self.timeStamp = 0

	def __str__(self):
		# note data in records do not change!
		return self.name + str(" %d/%d/%d %d:%d:%d, %.8f, %.8f , %.8f" % (self.year, self.month, self.day, self.hour, self.minute, self.second, self.latitude, self.longitude,self.altitude))

	def read(self):
		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH h5bhbdddffddddhh2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		s = rec_unpack(self.fileptr.read(rec_len))
		
		# intro parameters
		self.groupStart			= s[0]
		self.groupID			= s[1]
		self.byteCount			= s[2]

		# time types structure dddBB
		self.transactionNumber	= s[3]
		self.hour				= s[4]
		self.minute				= s[5]
		self.second				= s[6]
		self.month				= s[7]
		self.day				= s[8]
		self.year				= s[9]
		self.alignmentStatus	= s[10]
		self.latitude			= s[11]
		self.longitude			= s[12]
		self.altitude			= s[13] 
		self.horizontalPositionCEP	= s[14]
		self.initialAltitudeRMS	= s[15]
		self.initialDistance	= s[16]
		self.initialRoll		= s[17]
		self.initialPitch		= s[18]
		self.initialHeading		= s[19]				
		self.pad				= s[20]
		self.checksum			= s[21]
		self.groupEnd			= s[22]
		
		self.timeStamp = to_timestamp(datetime.datetime(self.year, self.month, self.day, self.hour, self.minute, self.second)) + self.timeOrigin
		
###############################################################################
class C_110:
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.name = "MV General Status & FDIR - Extension (110)"
		self.typeOfDatagram = 110
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin
		self.timeStamp = 0

	def __str__(self):
		return "General Status (110), " + str(from_timestamp(self.timeStamp)) + ", " + self.data

	def read(self):
		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH dddBB hhhh2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))

		self.groupStart			= s[0]
		self.groupID			= s[1]
		self.byteCount			= s[2]

		# time types structure dddBB
		self.timeStamp			= s[3] + self.timeOrigin
		self.time2				= s[4] + self.timeOrigin
		self.distanceTag		= s[5]
		self.timeTypes			= s[6]
		self.distanceTypes		= s[7]

		self.generalStatus		= s[8]
		self.trueZtimeRemaining	= s[9]
		self.pad				= s[10]
		self.checksum			= s[11]
		self.groupEnd			= s[12]

		self.data = "Warning, status on TrueZ not set"
		if isBitSet(self.generalStatus, 0):
			self.data += "User logged in,"
		if isBitSet(self.generalStatus, 10):
			self.data += "TrueZ active,"
		if isBitSet(self.generalStatus, 11):
			self.data += "TrueZ ready,"
		if isBitSet(self.generalStatus, 12):
			self.data += "TrueZ inuse"

###############################################################################
class C_111: 
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.name = "TrueHeave (111)"
		self.typeOfDatagram = 111
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin
		self.timeStamp = 0

	def header(self):
		return "HeaveDate, HeaveTimeStamp, Heave, HeaveRMS, TrueHeaveDate, TrueHeaveTimeStamp, TrueHeave, TrueHeaveRMS, RejectedIMUCount"

	def __str__(self):
		return from_timestamp(self.timeStamp).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + str(", %.3f, %.3f, %.3f, " % (self.timeStamp, self.heave, self.heaveRMS)) + from_timestamp(self.heaveTime1).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + str(", %.3f, %.3f, %.3f, %d" % (self.heaveTime1, self.trueHeave, self.trueHeaveRMS, self.rejectedIMUCount))

	def read(self):
		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH dddBB ffLffddLLhh2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))
		
		# intro parameters
		self.groupStart			= s[0]
		self.groupID			= s[1]
		self.byteCount			= s[2]

		# time types structure dddBB
		self.timeStamp			= s[3] + self.timeOrigin
		self.time2				= s[4] + self.timeOrigin
		self.distanceTag		= s[5]
		self.timeTypes			= s[6]
		self.distanceTypes		= s[7]

		self.trueHeave			= s[8]
		self.trueHeaveRMS		= s[9]
		self.status				= s[10]
		self.heave				= s[11]
		self.heaveRMS			= s[12]
		self.heaveTime1			= s[13] + self.timeOrigin
		self.heaveTime2			= s[14] + self.timeOrigin
		self.rejectedIMUCount	= s[15]
		self.outOfRangeIMUCount	= s[16]
		
		self.pad				= s[17]
		self.checksum			= s[18]
		self.groupEnd			= s[19]

###############################################################################
class C_14:
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.typeOfDatagram = 14
		self.name = getDatagramName(self.typeOfDatagram)
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin

	def __str__(self):
		return self.name + from_timestamp(self.time1).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + str(",%.3f,%.3f,%.3f,%.3f,%d" % (self.heave, self.trueHeave, self.heaveRMS, self.trueHeaveRMS, self.rejectedIMUCount))

	def read(self):
		self.fileptr.seek(self.offset, 0)
		self.data = self.fileptr.read(self.numberOfBytes)

		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH dddBB BBLHBB HBB H3B 4B 4B 6B Bh2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))
		print(s)

###############################################################################
class C_20:#MSG 20General Installation
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.typeOfDatagram = 20
		self.name = getDatagramName(self.typeOfDatagram)
		self.offset = fileptr.tell() 			# remember where we started
		self.numberOfBytes = numberOfBytes 		# remember the number of bytes in this record
		self.fileptr = fileptr 					# remember the file pointer
		self.fileptr.seek(numberOfBytes, 1)  	# jump to the end of the record so we can autoread the next one
		self.data = ""							# a byte bucket just in case
		self.timeOrigin = timeOrigin 			# remember the time origin sowe can compute the correct time based on GPS week

	def header(self):
		return "Ref2IMU_X, Ref2IMU_Y, Ref2IMU_Z, Ref2PrimaryGPS_X, Ref2PrimaryGPS_Y, Ref2PrimaryGPS_Z, Ref2Aux1GPS_X, Ref2Aux1GPS_Y, Ref2Aux1GPS_Z, Ref2Aux2GPS_X, Ref2Aux2GPS_Y, Ref2Aux2GPS_Z, IMU2RefFrameAngle_X, IMU2RefFrameAngle_Y, IMU2RefFrameAngle_Z, RefFrame2VesselFrameAngle_X, RefFrame2VesselFrameAngle_Y, RefFrame2VesselFrameAngle_Z "

	def __str__(self):
		return str("%.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f,%.3f, %.3f, %.3f, %.3f, %.3f, %.3f,   " % (self.Ref2IMU_X, self.Ref2IMU_Y, self.Ref2IMU_Z, self.Ref2PrimaryGPS_X, self.Ref2PrimaryGPS_Y, self.Ref2PrimaryGPS_Z, self.Ref2Aux1GPS_X, self.Ref2Aux1GPS_Y, self.Ref2Aux1GPS_Z, self.Ref2Aux2GPS_X, self.Ref2Aux2GPS_Y, self.Ref2Aux2GPS_Z, self.IMU2RefFrameAngle_X, self.IMU2RefFrameAngle_Y, self.IMU2RefFrameAngle_Z, self.RefFrame2VesselFrameAngle_X, self.RefFrame2VesselFrameAngle_Y, self.RefFrame2VesselFrameAngle_Z))
		

	def read(self):
		self.fileptr.seek(self.offset, 0)
		self.data = self.fileptr.read(self.numberOfBytes)

		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH HB BB 18f B hH2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		s = rec_unpack(self.fileptr.read(rec_len))
		self.Ref2IMU_X						= s[7]
		self.Ref2IMU_Y						= s[8]
		self.Ref2IMU_Z						= s[9]
		self.Ref2PrimaryGPS_X				= s[10]
		self.Ref2PrimaryGPS_Y				= s[11]
		self.Ref2PrimaryGPS_Z				= s[12]
		self.Ref2Aux1GPS_X					= s[13]
		self.Ref2Aux1GPS_Y					= s[14]
		self.Ref2Aux1GPS_Z					= s[15]
		self.Ref2Aux2GPS_X					= s[16]
		self.Ref2Aux2GPS_Y					= s[17]
		self.Ref2Aux2GPS_Z					= s[18]
		self.IMU2RefFrameAngle_X			= s[19]
		self.IMU2RefFrameAngle_Y			= s[20]
		self.IMU2RefFrameAngle_Z			= s[21]
		self.RefFrame2VesselFrameAngle_X	= s[22]
		self.RefFrame2VesselFrameAngle_Y	= s[23]
		self.RefFrame2VesselFrameAngle_Z	= s[24]

		# print (s)
###############################################################################
class C_29:
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.typeOfDatagram = 29
		self.name = getDatagramName(self.typeOfDatagram)
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin

	def __str__(self):
		return self.name + from_timestamp(self.time1).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + str(",%.3f,%.3f,%.3f,%.3f,%d" % (self.heave, self.trueHeave, self.heaveRMS, self.trueHeaveRMS, self.rejectedIMUCount))

	def read(self):
		self.fileptr.seek(self.offset, 0)
		self.data = self.fileptr.read(self.numberOfBytes)

		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4shh dddBB BBLHBB HBB H3B 4B 4B 6B Bh2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))
		print(s)


###############################################################################
class C_112:
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.name = "NMEA Strings (112)"
		self.typeOfDatagram = 112
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin

	def __str__(self):
		return self.name + from_timestamp(self.time1).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + str(",%.3f,%.3f,%.3f,%.3f,%d" % (self.heave, self.trueHeave, self.heaveRMS, self.trueHeaveRMS, self.rejectedIMUCount))

	def read(self):
		self.fileptr.seek(self.offset, 0)
		self.data = self.fileptr.read(self.numberOfBytes)

		# halt development as we have no example for testing...
		# self.fileptr.seek(self.offset, 0)
		# rec_fmt = '=4sHH dddBB H'
		# rec_len = struct.calcsize(rec_fmt)
		# rec_unpack = struct.Struct(rec_fmt).unpack
		# # bytesRead = rec_len
		# s = rec_unpack(self.fileptr.read(rec_len))
		
		# # intro parameters
		# self.groupStart			= s[0]
		# self.groupID			= s[1]
		# self.byteCount			= s[2]

		# # time types structure dddBB
		# self.time1				= s[3] + self.timeOrigin
		# self.time2				= s[4] + self.timeOrigin
		# self.distanceTag		= s[5]
		# self.timeTypes			= s[6]
		# self.distanceTypes		= s[7]
		# self.variableGroupByteCount = s[9]
		# self.NMEA		= s[10]
		
		# self.pad				= s[12]
		# self.checksum			= s[12]
		# self.groupEnd			= s[13]

###############################################################################
class C_4: 
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.typeOfDatagram = 4
		self.name = getDatagramName(self.typeOfDatagram)
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin
		self.timeStamp = 0

	# def header(self):
	# 	return "Date, Latitude, Longitude, Altitude, Pitch, Roll, Heading, Speed"
		
	def __str__(self):
		return from_timestamp(self.timeStamp).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + " unpublished format"

	def read(self):
		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH dddBB 29s B H2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))
		
		# intro parameters 4shh
		self.groupStart				= s[0]
		self.groupID				= s[1]
		self.byteCount				= s[2]

		# time types structure dddBB
		self.timeStamp				= s[3] + self.timeOrigin
		self.time2					= s[4] + self.timeOrigin
		self.distanceTag			= s[5]
		self.timeTypes				= s[6]
		self.distanceTypes			= s[7]

		#need to read 29 bytes of IMU here...

		#footer BH2s
		# self.pad				= s[16]
		# self.checksum				= s[17]
		# self.groupEnd				= s[18]
###############################################################################
class C_1: 
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.name = "Position, Velocity, Attitude (GRP:1)"
		self.typeOfDatagram = 1
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin
		self.timeStamp = 0

	def header(self):
		return "Date, Latitude, Longitude, Altitude, Pitch, Roll, Heading, Speed"
		
	def __str__(self):
		return from_timestamp(self.timeStamp).strftime('%Y/%m/%d %H:%M:%S.%f')[:-3] + str(" %.10f, %.10f, %.3f, %.3f, %.3f, %.3f, %.3f" % (self.latitude, self.longitude, self.altitude, self.vesselPitch, self.vesselRoll, self.vesselHeading, self.vesselSpeed))

	def read(self):
		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH dddBB dddfffdddd8fbbH2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))
		
		# intro parameters
		self.groupStart			= s[0]
		self.groupID			= s[1]
		self.byteCount			= s[2]

		# time types structure dddBB
		self.timeStamp				= s[3] + self.timeOrigin
		self.time2					= s[4] + self.timeOrigin
		self.distanceTag			= s[5]
		self.timeTypes				= s[6]
		self.distanceTypes			= s[7]

		self.latitude				= s[8]
		self.longitude				= s[9]
		self.altitude				= s[10]

		self.northVelocity			= s[11]
		self.eastVelocity			= s[12]
		self.downVelocity			= s[13]
		self.vesselRoll				= s[14]
		self.vesselPitch			= s[15]
		self.vesselHeading			= s[16]
		self.vesselWanderAngle		= s[17]
		
		self.vesselTrackAngle		= s[18]
		self.vesselSpeed			= s[19]
		self.vesselAngularRateLongitudinal			= s[20]
		self.vesselAngularRateTransverse			= s[21]
		self.vesselAngularRateDown			= s[22]

		self.vesselLongitudinalAccel			= s[23]
		self.vesselTransversAccel			= s[24]
		self.vesselDownAccel			= s[25]

		self.checksum				= s[26]
		self.groupEnd				= s[27]
###############################################################################
class C_10: 
	def __init__(self, fileptr, numberOfBytes, timeOrigin):
		self.name = "General FDIR Metrics (10)"
		self.typeOfDatagram = 10
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
		self.timeOrigin = timeOrigin
		self.timeStamp = 0

	def __str__(self):
		return self.name + str(from_timestamp(self.timeStamp)) + "," + self.data

	def read(self):
		self.fileptr.seek(self.offset, 0)
		rec_fmt = '=4sHH dddBB LLLLHHHHHLH2s'
		rec_len = struct.calcsize(rec_fmt)
		rec_unpack = struct.Struct(rec_fmt).unpack
		# bytesRead = rec_len
		s = rec_unpack(self.fileptr.read(rec_len))
		
		# intro parameters
		self.groupStart			= s[0]
		self.groupID			= s[1]
		self.byteCount			= s[2]

		# time types structure dddBB
		self.timeStamp				= s[3] + self.timeOrigin
		self.time2					= s[4] + self.timeOrigin
		self.distanceTag			= s[5]
		self.timeTypes				= s[6]
		self.distanceTypes			= s[7]

		self.generalStatusA			= s[8]
		self.generalStatusB			= s[9]
		self.generalStatusC			= s[10]
		self.FDIRLevel1Status		= s[11]
		self.FDIRLevel1IMUFailures	= s[12]
		self.FDIRLevel2Status		= s[13]
		self.FDIRLevel3Status		= s[14]
		self.FDIRLevel4Status		= s[15]
		self.FDIRLevel5Status		= s[16]
		self.extendedStatus			= s[17]
		
		self.checksum				= s[18]
		self.groupEnd				= s[19]

		if isBitSet(self.FDIRLevel1Status, 0):
			self.data += "**IMU-POS checksum error, "
		if isBitSet(self.FDIRLevel1Status, 1):
			self.data += "**IMU status bit set by IMU, "
		if isBitSet(self.FDIRLevel1Status, 2):
			self.data += "**Successive IMU failures, "
		if isBitSet(self.FDIRLevel1Status, 3):
			self.data += "**IIN configuration mismatch failure, "
		if isBitSet(self.FDIRLevel1Status, 45):
			self.data += "**Primary GPS not in Navigation mode, "
		if isBitSet(self.FDIRLevel1Status, 6):
			self.data += "**Primary GPS not available for alignment, "
		if isBitSet(self.FDIRLevel1Status, 7):
			self.data += "**Primary data gap, "
		if isBitSet(self.FDIRLevel1Status, 8):
			self.data += "**Primary GPS PPS time gap, "
		if isBitSet(self.FDIRLevel1Status, 9):
			self.data += "**Primary GPS time recovery data not received, "
		if isBitSet(self.FDIRLevel1Status, 10):
			self.data += "**Primary GPS observable data gap, "
		if isBitSet(self.FDIRLevel1Status, 11):
			self.data += "**Primary ephemeris data gap, "
		if isBitSet(self.FDIRLevel1Status, 13):
			self.data += "**Primary GPS missing ephemeris, "
		if isBitSet(self.FDIRLevel1Status, 20):
			self.data += "**Secondary GPS data gap, "
		if isBitSet(self.FDIRLevel1Status, 21):
			self.data += "**Secondary GPS observable data gap, "
		if isBitSet(self.FDIRLevel1Status, 25):
			self.data += "Auxiliary GPS data gap, "
		if isBitSet(self.FDIRLevel1Status, 26):
			self.data += "**GAMS ambiguity resolution failed, "
		if isBitSet(self.FDIRLevel1Status, 30):
			self.data += "**IIN WL ambiguity error, "
		if isBitSet(self.FDIRLevel1Status, 31):
			self.data += "**IIN NL ambiguity error, "

		if isBitSet(self.FDIRLevel4Status, 0):
			self.data += "**Primary GPS position rejected, "
		if isBitSet(self.FDIRLevel4Status, 1):
			self.data += "**Primary GPS velocity rejected, "
		if isBitSet(self.FDIRLevel4Status, 2):
			self.data += "**GAMS heading rejected, "
		if isBitSet(self.FDIRLevel4Status, 3):
			self.data += "**Auxiliary GPS data rejected, "
		if isBitSet(self.FDIRLevel4Status, 5):
			self.data += "**Primary GPS observables rejected, "

		if isBitSet(self.FDIRLevel5Status, 0):
			self.data += "**X accelerometer failure, "
		if isBitSet(self.FDIRLevel4Status, 1):
			self.data += "**Y accelerometer failure, "
		if isBitSet(self.FDIRLevel4Status, 2):
			self.data += "**Z accelerometer failure, "
		if isBitSet(self.FDIRLevel4Status, 3):
			self.data += "**X gyro failure, "
		if isBitSet(self.FDIRLevel4Status, 4):
			self.data += "**Y gyro failure, "
		if isBitSet(self.FDIRLevel4Status, 5):
			self.data += "**Z gyro failure, "
		if isBitSet(self.FDIRLevel4Status, 6):
			self.data += "**Excessive GAMS heading offset, "
		if isBitSet(self.FDIRLevel4Status, 7):
			self.data += "**Excessive primary GPS lever arm error, "
		if isBitSet(self.FDIRLevel4Status, 8):
			self.data += "**Excessive auxiliary 1 GPS lever arm error, "
		if isBitSet(self.FDIRLevel4Status, 9):
			self.data += "**Excessive auxiliary 2 GPS lever arm error, "
		if isBitSet(self.FDIRLevel4Status, 10):
			self.data += "**Excessive POS position error RMS, "
		if isBitSet(self.FDIRLevel4Status, 11):
			self.data += "**Excessive primary GPS clock drift, "

		# now decode into a string.
		if isBitSet(self.generalStatusA, 0):
			self.data += "Coarse levelling active, "
		if isBitSet(self.generalStatusA, 1):
			self.data += "Coarse levelling failed, "
		if isBitSet(self.generalStatusA, 2):
			self.data += "Quadrant resolved, "
		if isBitSet(self.generalStatusA, 3):
			self.data += "Fine align active, "
		if isBitSet(self.generalStatusA, 4):
			self.data += "Inertial navigator initialised, "
		if isBitSet(self.generalStatusA, 5):
			self.data += "Inertial navigator alignment active, "
		if isBitSet(self.generalStatusA, 6):
			self.data += "Degraded navigation solution, "
		if isBitSet(self.generalStatusA, 7):
			self.data += "Full navigation solution, "
		if isBitSet(self.generalStatusA, 8):
			self.data += "Initial position valid, "
		if isBitSet(self.generalStatusA, 9):
			self.data += "Reference to Primary GPS Lever arms = 0, "
		if isBitSet(self.generalStatusA, 10):
			self.data += "Reference to Sensor 1 Lever arms = 0, "
		if isBitSet(self.generalStatusA, 11):
			self.data += "Reference to Sensor 2 Lever arms = 0, "
		if isBitSet(self.generalStatusA, 12):
			self.data += "Logging Port file write error, "
		if isBitSet(self.generalStatusA, 13):
			self.data += "Logging Port file open, "
		if isBitSet(self.generalStatusA, 14):
			self.data += "Logging Port logging enabled, "
		if isBitSet(self.generalStatusA, 15):
			self.data += "Logging Port device full, "
		if isBitSet(self.generalStatusA, 16):
			self.data += "RAM configuration differs from NVM, "
		if isBitSet(self.generalStatusA, 17):
			self.data += "NVM write successful, "
		if isBitSet(self.generalStatusA, 18):
			self.data += "NVM write fail, "
		if isBitSet(self.generalStatusA, 19):
			self.data += "NVM read fail, "
		if isBitSet(self.generalStatusA, 20):
			self.data += "CPU loading exceeds 55% threshold, "
		if isBitSet(self.generalStatusA, 21):
			self.data += "CPU loading exceeds 85% threshold, "

		if isBitSet(self.generalStatusB, 0):
			self.data += "User attitude RMS performance, "
		if isBitSet(self.generalStatusB, 1):
			self.data += "User heading RMS performance, "
		if isBitSet(self.generalStatusB, 2):
			self.data += "User position RMS performance, "
		if isBitSet(self.generalStatusB, 3):
			self.data += "User velocity RMS performance, "
		if isBitSet(self.generalStatusB, 4):
			self.data += "GAMS calibration in progress, "
		if isBitSet(self.generalStatusB, 5):
			self.data += "GAMS calibration complete, "
		if isBitSet(self.generalStatusB, 6):
			self.data += "GAMS calibration failed, "
		if isBitSet(self.generalStatusB, 7):
			self.data += "GAMS calibration requested, "
		if isBitSet(self.generalStatusB, 8):
			self.data += "GAMS installation parameters valid, "
		if isBitSet(self.generalStatusB, 9):
			self.data += "GAMS solution in use, "
		if isBitSet(self.generalStatusB, 10):
			self.data += "GAMS solution OK, "
		if isBitSet(self.generalStatusB, 11):
			self.data += "GAMS calibration suspended, "
		if isBitSet(self.generalStatusB, 12):
			self.data += "GAMS calibration forced, "
		if isBitSet(self.generalStatusB, 13):
			self.data += "Primary GPS navigation solution in use, "
		if isBitSet(self.generalStatusB, 14):
			self.data += "Primary GPS initialization failed, "
		if isBitSet(self.generalStatusB, 15):
			self.data += "Primary GPS reset command sent, "
		if isBitSet(self.generalStatusB, 16):
			self.data += "Primary GPS configuration file sent, "
		if isBitSet(self.generalStatusB, 17):
			self.data += "Primary GPS not configured, "
		if isBitSet(self.generalStatusB, 18):
			self.data += "Primary GPS in C/A mode, "
		if isBitSet(self.generalStatusB, 19):
			self.data += "Primary GPS in Differential mode, "
		if isBitSet(self.generalStatusB, 20):
			self.data += "Primary GPS in float RTK mode, "
		if isBitSet(self.generalStatusB, 21):
			self.data += "Primary GPS in wide lane RTK mode, "
		if isBitSet(self.generalStatusB, 22):
			self.data += "Primary GPS in narrow lane RTK mode, "
		if isBitSet(self.generalStatusB, 23):
			self.data += "Primary GPS observables in use, "
		if isBitSet(self.generalStatusB, 24):
			self.data += "Secondary GPS observables in use, "
		if isBitSet(self.generalStatusB, 25):
			self.data += "Auxiliary GPS navigation solution in use, "
		if isBitSet(self.generalStatusB, 26):
			self.data += "Auxiliary GPS in P-code mode, "
		if isBitSet(self.generalStatusB, 27):
			self.data += "Auxiliary GPS in Differential mode, "
		if isBitSet(self.generalStatusB, 28):
			self.data += "Auxiliary GPS in float RTK mode, "
		if isBitSet(self.generalStatusB, 29):
			self.data += "Auxiliary GPS in wide lane RTK mode, "
		if isBitSet(self.generalStatusB, 20):
			self.data += "Auxiliary GPS in narrow lane RTK mode, "
		if isBitSet(self.generalStatusB, 31):
			self.data += "Primary GPS in P-code mode, "

		if isBitSet(self.generalStatusC, 0):
			self.data += "Gimbal input ON, "
		if isBitSet(self.generalStatusC, 1):
			self.data += "Gimbal data in use, "
		if isBitSet(self.generalStatusC, 2):
			self.data += "DMI data in use, "
		if isBitSet(self.generalStatusC, 3):
			self.data += "ZUPD processing enabled, "
		if isBitSet(self.generalStatusC, 4):
			self.data += "ZUPD in use, "
		if isBitSet(self.generalStatusC, 5):
			self.data += "Position fix in use, "
		if isBitSet(self.generalStatusC, 6):
			self.data += "RTCM differential corrections in use, "
		if isBitSet(self.generalStatusC, 7):
			self.data += "RTCM RTK messages in use, "
		if isBitSet(self.generalStatusC, 8):
			self.data += "RTCA RTK messages in use, "
		if isBitSet(self.generalStatusC, 9):
			self.data += "CMR RTK messages in use, "
		if isBitSet(self.generalStatusC, 10):
			self.data += "IIN in DR mode, "
		if isBitSet(self.generalStatusC, 11):
			self.data += "IIN GPS aiding is loosely coupled, "
		if isBitSet(self.generalStatusC, 12):
			self.data += "IIN in C/A GPS aided mode, "
		if isBitSet(self.generalStatusC, 13):
			self.data += "IIN in RTCM DGPS aided mode, "
		if isBitSet(self.generalStatusC, 14):
			self.data += "IIN in code DGPS aided mode, "
		if isBitSet(self.generalStatusC, 15):
			self.data += "IIN in float RTK aided mode, "
		if isBitSet(self.generalStatusC, 16):
			self.data += "IIN in wide lane RTK aided mode, "
		if isBitSet(self.generalStatusC, 17):
			self.data += "IIN in narrow lane RTK aided mode, "
		if isBitSet(self.generalStatusC, 18):
			self.data += "Received RTCM Type 1 message, "
		if isBitSet(self.generalStatusC, 19):
			self.data += "Received RTCM Type 3 message, "
		if isBitSet(self.generalStatusC, 20):
			self.data += "Received RTCM Type 9 message, "
		if isBitSet(self.generalStatusC, 21):
			self.data += "Received RTCM Type 18 messages, "
		if isBitSet(self.generalStatusC, 22):
			self.data += "Received RTCM Type 19 messages, "
		if isBitSet(self.generalStatusC, 23):
			self.data += "Received CMR Type 0 message, "
		if isBitSet(self.generalStatusC, 24):
			self.data += "Received CMR Type 1 message, "
		if isBitSet(self.generalStatusC, 25):
			self.data += "Received CMR Type 2 message, "
		if isBitSet(self.generalStatusC, 26):
			self.data += "Received CMR Type 94 message, "
		if isBitSet(self.generalStatusC, 27):
			self.data += "Received RTCA SCAT-1 messageV, "

		if isBitSet(self.extendedStatus, 0):
			self.data += "Primary GPS in Marinestar HP mode, "
		if isBitSet(self.extendedStatus, 1):
			self.data += "Primary GPS in Marinestar XP mode, "
		if isBitSet(self.extendedStatus, 2):
			self.data += "Primary GPS in Marinestar VBS mode, "
		if isBitSet(self.extendedStatus, 3):
			self.data += "Primary GPS in PPP mode, "
		if isBitSet(self.extendedStatus, 4):
			self.data += "Aux. GPS in Marinestar HP mode, "
		if isBitSet(self.extendedStatus, 5):
			self.data += "Aux. GPS in Marinestar XP mode, "
		if isBitSet(self.extendedStatus, 6):
			self.data += "Aux. GPS in Marinestar VBS mode, "
		if isBitSet(self.extendedStatus, 7):
			self.data += "Aux. GPS in PPP mode, "
		if isBitSet(self.extendedStatus, 12):
			self.data += "Primary GPS in Marinestar G2 mode, "
		if isBitSet(self.extendedStatus, 14):
			self.data += "Primary GPS in Marinestar HPXP mode, "
		if isBitSet(self.extendedStatus, 15):
			self.data += "Primary GPS in Marinestar HPG2 mode, "


###############################################################################
class POSReader:
	'''class to read a Applanix .000 file'''
	# group start 	4 chars
	# group id		2 ushort
	# byte count	2 ushort
	# time1			8 double
	# time2			8 double
	# disatance tag	8 double
	# time type		1 B
	# distance type	1 B
	# pad			0-3
	# checksum		2 ushort
	# group end		2 char
	#  time distance fields are dddBB
	POSPacketHeader_fmt = '=4sHHdddBB'
	POSPacketHeader_len = struct.calcsize(POSPacketHeader_fmt)
	POSPacketHeader_unpack = struct.Struct(POSPacketHeader_fmt).unpack_from

###############################################################################
	def __init__(self, POSfileName):
		if not os.path.isfile(POSfileName):
			print ("file not found:", POSfileName)
		if os.path.isdir(POSfileName):
			print ("this is a folder, skipping...", POSfileName)
			return

		self.fileName = POSfileName
		self.fileptr = open(POSfileName, 'rb')		
		self.fileSize = os.path.getsize(POSfileName)
		self.fileStartDateObject = 0 #date object
		self.recordTimeStamp = 0 #UTC unixtime
		self.timeOrigin = 0 #UTC time origin for the current file (GPSWeek Number in UTC seconds sonce 1970 for a POSMV file, to which we add the fractional seconds in each record)

###############################################################################
	def __str__(self):
		return pprint.pformat(vars(self))

###############################################################################
	def findGPSWeek(self):
		haveWeek = False
		haveSeconds = False
		# now try and parse the filename for the GPS week.  There may be no datagram with this!
		# 1. the documentation suggests we find the GPSweek in the group 3 message.  Unfortunately it may not be there
		# 2. if no group 3 message, we can look at the MSG10 (General Data) message, which has a time data at 1Hz.  
		#	This is not a bad one to set the GPSweek and our self.timeOrigin as the file may roll over into a new week (pkpk lets test this to see if true)
		# 3. As a fallback, we can parse the filename.  If the user runs wioth the default, the filename has the data within the filename itself.
		#	default filename is in the format 20170403_0138.000, 20170403_0138.001 etc

		try:
			base = os.path.basename(self.fileName)
			name = os.path.splitext(base)[0]

			digits = [s for s in re.findall(r'-?\d+\.?\d*', name)]
			for d in digits:
				if len(d) == 8:
					if d[0] == '1': # a patch for badly formed filenames
						d = d[1:]
					self.fileStartDateObject = datetime.datetime.strptime(d[:8],"%Y%m%d")
					self.week, gpsWeekinUTCSeconds, gpsWeekInGPSSeconds, gpsDayofWeek, gpsSecondsOfWeek, microseconds = self.utcToWeekSeconds(self.fileStartDateObject, 0)
					self.timeOrigin = gpsWeekinUTCSeconds
					# date = self.weekSecondsToUtc(self.week, 0,0)
					# print ("FileName: %s GPS Week: %d %s" % (self.fileName, self.week, date))
					return

			# self.fileStartDateObject = datetime.datetime.strptime(name[0:8],"%Y%m%d")
			# self.week, gpsWeekinUTCSeconds, gpsWeekInGPSSeconds, gpsDayofWeek, gpsSecondsOfWeek, microseconds = self.utcToWeekSeconds(self.fileStartDateObject, 0)
			# self.timeOrigin = gpsWeekinUTCSeconds

			# # self.weekInSeconds = self.utcToWeekSeconds
			# date = self.weekSecondsToUtc(self.week, 0,0)
			# # print ("FileName: %s GPS Week: %d %s" % (self.fileName, self.week, date))
			# return
		except:
			self.week = 0
			print ("filename does not contain a valid GPS week signature, timestamp correction needs to come from a datagram: ", self.fileName)

		while self.moreData():

			# read a datagram.  If we support it, return the datagram type and aclass for that datagram
			# The user then needs to cPOS the read() method for the class to undertake a fileread and binary decode.  This keeps the read super quick.
			groupID, datagram = self.readDatagram()
			if groupID==False:
				self.fileStartDateObject = datetime.datetime(1970,1,1)
				return
			# Decode group 3 message for the GPSweek pkpk we can only do this once we have one for testing.  until then use the MESSAGE 10 record even though it is not very reliable
			# pkpk  todo

			# Decode MESSAGE 10 message for the GPSweek
			# Note this does not change very regularly (if ever since start up) so is a very unreliable source
			if (groupID == 56): # "MESSAGE56: General Data" 
				datagram.read()
				# self.fileStartDateObject  = datetime.datetime(datagram.year, datagram.month, datagram.day, datagram.hour, datagram.minute, int(datagram.second))				
				#no point pretending this is more accurate.  It is not!
				self.fileStartDateObject  = datetime.datetime(datagram.year, datagram.month, datagram.day)
				self.week, gpsWeekinUTCSeconds, gpsWeekInGPSSeconds, gpsDayofWeek, gpsSecondsOfWeek, microseconds = self.utcToWeekSeconds(self.fileStartDateObject, 0)
				self.timeOrigin = gpsWeekinUTCSeconds
				self.fileStartDateObject = from_timestamp(self.timeOrigin + self.recordTimeStamp)
			# 	haveWeek = True
			# if (groupID == 1): # "GRP1: Position & Velocity" #does not havew week number!!
			# 	datagram.read()
			# 	# self.fileStartDateObject
			# 	# dd = from_timestamp(datagram.timeStamp)
			# 	haveSeconds = True
			# if haveWeek and haveSeconds:
				# we have a record, so quit
				self.rewind()
				return
		

###############################################################################
	def currentfileStartDateObjectTime(self):
		'''return a python date object from the current datagram objects raw date and time fields '''
		# date_object = datetime.strptime(str(self.fileStartDateObject), '%Y%m%d') + timedelta(0,self.recordTimeStamp)
		date_object = from_timestamp(self.recordTimeStamp)
		return date_object

###############################################################################
	# def to_DateTime(self, fileStartDateObject, recordTimeStamp):
	# 	'''return a python date object from a split date and time record'''
	# 	date_object = datetime.strptime(str(fileStartDateObject), '%Y%m%d') + timedelta(0,recordTimeStamp)
	# 	return date_object

###############################################################################
# https://stackoverflow.com/questions/45422739/gps-time-in-weeks-since-epoch-in-python	
# utctoweekseconds(datetime.datetime.strptime('2014-09-22 21:36:52',"%Y-%m-%d %H:%M:%S"),16)
# gives: (1811, 1, 164196,0)
###############################################################################
###############################################################################
	def weekSecondsToUtc(self, gpsweek, gpsseconds, leapseconds):
		datetimeformat = "%Y-%m-%d %H:%M:%S"
		epoch = datetime.datetime.strptime("1980-01-06 00:00:00",datetimeformat)
		elapsed = datetime.timedelta(days=(gpsweek*7),seconds=(gpsseconds+leapseconds))
		return datetime.datetime.strftime(epoch + elapsed,datetimeformat)

	def utcToWeekSeconds(self, utcDate, leapseconds):
		""" Returns the GPS week, the GPS day, and the seconds 
			and microseconds since the beginning of the GPS week """
		datetimeformat = "%Y-%m-%d %H:%M:%S"
		epoch = datetime.datetime.strptime("1980-01-06 00:00:00",datetimeformat)
		# tdiff = utcDate - epoch - datetime.timedelta(seconds=leapseconds)
		tdiff = utcDate - epoch
		gpsweek = tdiff.days // 7 
		gpsDayofWeek = tdiff.days - (7 * gpsweek)
		gpsSecondsOfWeek = tdiff.seconds + 86400* (tdiff.days - (7 * gpsweek))
		gpsWeekInGPSSeconds = 86400* (7 * gpsweek)
		UnixTimeOrigin = datetime.datetime(1970,1,1)
		UnixTimeOrigin = datetime.datetime(1970,1,1)
		
		gpsWeekInUTCSeconds = gpsWeekInGPSSeconds + (epoch - UnixTimeOrigin).total_seconds()

		# date = self.weekSecondsToUtc(gpsweek, gpsSecondsOfWeek, leapseconds)

		return gpsweek, gpsWeekInUTCSeconds, gpsWeekInGPSSeconds, gpsDayofWeek, gpsSecondsOfWeek,  tdiff.microseconds

###############################################################################
	def readDatagramHeader(self):
			'''read the common header for any datagram'''
			try:
				curr = self.fileptr.tell()
				data = self.fileptr.read(self.POSPacketHeader_len)
				s = self.POSPacketHeader_unpack(data)
				# now reset file pointer
				self.fileptr.seek(curr, 0)

				groupStart		= s[0]
				groupID			= s[1]
				numberOfBytes   = s[2]

				# we are dealing with messages rather than groups, so the format after the first 3 params is different, so quit.
				if groupStart.decode("utf-8") == "$MSG":
					# print ("message:", groupID)
					return numberOfBytes + 8, groupID, self.recordTimeStamp

				# self.fileStartDateObject			= s[3] + self.timeOrigin #GPS seconds of the week using user prefernece.  We normally use this and the default is fine
				# self.recordTimeStamp			= s[4] + self.timeOrigin #GPS seconds of the week in POS time (time since startup)
				self.recordTimeStamp			= s[4] + self.timeOrigin
				# distanceTag		= s[5]
				# timeTypes		= s[6]
								
				return numberOfBytes + 8, groupID, self.recordTimeStamp
			except:
				return -1,0,0

###############################################################################
	def close(self):
		'''close the current file'''
		self.fileptr.close()
		
###############################################################################
	def rewind(self):
		'''go back to start of file'''
		self.fileptr.seek(0, 0)				
	
###############################################################################
	def currentPtr(self):
		'''report where we are in the file reading process'''
		return self.fileptr.tell()

###############################################################################
	def moreData(self):
		'''report how many more bytes there are to read from the file'''
		return self.fileSize - self.fileptr.tell()
			
###############################################################################
	def readDatagramBytes(self, offset, byteCount):
		'''read the entire raw bytes for the datagram without changing the file pointer.  this is used for file conditioning'''
		curr = self.fileptr.tell()
		self.fileptr.seek(offset, 0)   # move the file pointer to the start of the record so we can read from disc			  
		data = self.fileptr.read(byteCount)
		self.fileptr.seek(curr, 0)
		return data

###############################################################################
	def getRecordCount(self):
		'''read through the entire file as fast as possible to get a count of POS records.  useful for progress bars so user can see what is happening'''
		count = 0
		self.rewind()
		while self.moreData():
			numberOfBytes, groupID, recordTimeStamp = self.readDatagramHeader()
			if numberOfBytes == -1:
				return False
			self.fileptr.seek(numberOfBytes, 1)
			count += 1
		self.rewind()		
		return count

###############################################################################
	def readDatagram(self):
		'''read the datagram header.  This permits us to skip datagrams we do not support'''
		numberOfBytes, groupID, recordTimeStamp = self.readDatagramHeader()
		if numberOfBytes == -1:
			return False, 0

		if groupID == 1: 
			dg = C_1(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 4: 
			dg = C_4(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 10: 
			dg = C_10(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 20: 
			dg = C_20(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 29: 
			dg = C_29(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 56: 
			dg = C_M56(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 110: 
			dg = C_110(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 111: 
			dg = C_111(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		if groupID == 112: 
			dg = C_112(self.fileptr, numberOfBytes, self.timeOrigin)
			return dg.typeOfDatagram, dg 
		
		dg = UNKNOWN_RECORD(self.fileptr, numberOfBytes, groupID)
		return dg.groupID, dg
			# self.fileptr.seek(numberOfBytes, 1)

###############################################################################
class UNKNOWN_RECORD:
	'''used as a convenience tool for datagrams we have no bespoke classes.  Better to make a bespoke class'''
	def __init__(self, fileptr, numberOfBytes, groupID):
		self.groupID = groupID
		self.offset = fileptr.tell()
		self.numberOfBytes = numberOfBytes
		self.fileptr = fileptr
		self.fileptr.seek(numberOfBytes, 1)
		self.data = ""
	def read(self):
		self.fileptr.seek(self.offset, 0)
		self.data = self.fileptr.read(self.numberOfBytes)

###############################################################################
def getDatagramName(groupID):
	'''Convert the datagram type from the code to a user readable string.  Handy for displaying to the user'''

	if (groupID == 1):
		return "GRP:01:Position, Attitude"
	if (groupID == 2):
		return "GRP:02:Navigation Performance Metric"
	if (groupID == 4):
		return "GRP04:IMU"
	if (groupID == 9):
		return "GRP9:GAMS Solution"
	if (groupID == 10):
		return "GRP10:General Status & FDIR"
	if (groupID == 20):
		return "GRP20:IIN Solution Status"
	if (groupID == 21):
		return "GRP:21:Base GPS 1 Modem"
	if (groupID == 24):
		return "GRP:24:Aux GPS"
	if (groupID == 29):
		return "GRP29:GNSS Receiver MarineSTAR Status"
	if (groupID == 32):
		return "MSG:32:Set POS IP Address"
	if (groupID == 33):
		return "MSG:33:Event Discrete Setup"
	if (groupID == 34):
		return "MSG:34:COM Port Setup"
	if (groupID == 35):
		return "MSG:35:NMEA Output Setup"
	if (groupID == 36):
		return "MSG:36:Binary Output Setup"
	if (groupID == 37):
		return "MSG:37:Base GPS1 Setup"
	if (groupID == 38):
		return "MSG:38:Base GPS2 Setup"
	if (groupID == 39):
		return "MSG:39:Aux GPS Setup"
	if (groupID == 41):
		return "MSG:41:Primary GPS Integrated DGPS Source Control"
	if (groupID == 50):
		return "MSG:50:Navigation Mode Control"
	if (groupID == 51):
		return "MSG:51:Display Port Control"
	if (groupID == 52):
		return "MSG:52:Realtime Date Port Control"
	if (groupID == 53):
		return "MSG:53:Logging Port Control"
	if (groupID == 56):
		return "MSG:56:General Data"
	if (groupID == 61):
		return "MSG:61:Loggin Data Port Control"
	if (groupID == 91):
		return "MSG:91:GPS Control"
	if (groupID == 92):
		return "Unknown92"
	if (groupID == 93):
		return "Unknown93"
	if (groupID == 99):
		return "GRP:99:Versions & Stats"
	if (groupID == 102):
		return "GRP:102:Sensor 1 Position, Attitude"
	if (groupID == 106):
		return "MSG:106:Heave Filter Setup"
	if (groupID == 110):
		return "GRP:110:MV General Status & FDIR"
	if (groupID == 111):
		return "GRP:111:True Heave"
	if (groupID == 112):
		return "GRP:112:NMEA"
	if (groupID == 113):
		return "GRP:113:True Heave Metrics"
	if (groupID == 114):
		return "GRP:114:TrueZ"
	if (groupID == 120):
		return "MSG:120:Sensor Parameter Setup"
	if (groupID == 135):
		return "MSG:135:NMEA Output Setup"
	if (groupID == 136):
		return "MSG:136:Binary Output Setup"
	if (groupID == 10001):
		return "GRP:10012:Primary GPS Stream"
	if (groupID == 20102):
		return "MSG:20102:Binary Output Diagnostics"


def isBitSet(int_type, offset):
	'''testBit() returns a nonzero result, 2**offset, if the bit at 'offset' is one.'''
	mask = 1 << offset
	return (int_type & (1 << offset)) != 0

def to_timestamp(fileStartDateObject):
	return (fileStartDateObject - datetime.datetime(1970, 1, 1)).total_seconds()

def from_timestamp(unixtime):
	return datetime.datetime(1970, 1 ,1) + timedelta(seconds=unixtime)
		

###############################################################################
def createOutputFileName(path):
	'''Create a valid output filename. if the name of the file already exists the file name is auto-incremented.'''
	path = os.path.expanduser(path)

	if not os.path.exists(os.path.dirname(path)):
		os.makedirs(os.path.dirname(path))

	if not os.path.exists(path):
		return path

	root, ext = os.path.splitext(os.path.expanduser(path))
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
def addFileNameAppendage(path, appendage):
	'''Create a valid output filename. if the name of the file already exists the file name is auto-incremented.'''
	path = os.path.expanduser(path)

	if not os.path.exists(os.path.dirname(path)):
		os.makedirs(os.path.dirname(path))

	if not os.path.exists(path):
		return path

	root, ext = os.path.splitext(os.path.expanduser(path))
	dir	   = os.path.dirname(root)
	fname	 = os.path.basename(root)
	candidate = "{}{}{}".format(fname, appendage, ext)

	return os.path.join(dir, candidate)

###############################################################################
def loadData(inputFiles, startTimeStamp, endTimeStamp):
	'''given a list of files or wildcard, and a start/end timestamp, efficiently find the POSMV files and then load them'''
	matches = []
	if os.path.exists(inputFiles):
		matches.append (os.path.abspath(inputFiles))
	else:
		for filename in glob(inputFiles):
			matches.append(filename)
	print (matches)

	if len(matches) == 0:
		print ("No files found in %s to process, quitting" % args.inputFile)
		exit()

	# now scan the files to see if it is within the desired time range
	for filename in matches:
		valid = False
		print ("Testing:" + filename)
		first, last = getFirstLastTimeStamps(filename, startTimeStamp)
		# test if first record is between .all range
		if startTimeStamp <= first <= endTimeStamp:
			valid = True
		# test if last record is between limits
		if startTimeStamp <= last <= endTimeStamp:
			valid = True
		# test for full overlap
		if first <= startTimeStamp and last >= endTimeStamp:
			valid = True
		if valid:
			print ("Found POSMV file for loading: %s" % (filename))
			heaveData = loadHeaveBetweenTimesStamps(filename, startTimeStamp, endTimeStamp)
			print ("Records Loaded: %d" %(len(heaveData)))


if __name__ == "__main__":
		main()
