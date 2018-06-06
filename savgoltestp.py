import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from numpy import genfromtxt
import csv
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from glob import glob

def main():
	parser = ArgumentParser(description='Readatime series CSV of time, heave remove the DC offset from the heave and write the result to a new CSV.',
			epilog='Example: \n -i c:/temp/myheave.csv \n', formatter_class=RawTextHelpFormatter)
	parser.add_argument('-i', dest='inputFile', action='store', help='Input csv filename to repair. this can also be a wildcard')
	parser.add_argument('-odir', dest='odir', action='store', default="", help='Specify a relative output folder e.g. -odir conditioned')
	parser.add_argument('-odix', dest='odix', action='store', default="_savgol", help='Specify an output filename appendage e.g. -odix _savgol')
	parser.add_argument('-level', dest='level', action='store', default="1000", help='Smoothing level (1-5000). [default: 1000')

	if len(sys.argv)==1:
		parser.print_help()
		sys.exit(1)
		
	args = parser.parse_args()
	matches = []

	if os.path.exists(args.inputFile):
		matches.append (os.path.abspath(args.inputFile))
	else:
		for filename in glob(args.inputFile):
			matches.append(filename)

	if len(matches) == 0:
		print ("Nothing found in %s to condition, quitting" % args.inputFile)
		exit()

	level = int(args.level)

	for filename in matches:
		# create an output file based on the input
		outFileName = os.path.join(os.path.dirname(os.path.abspath(filename)), args.odir, os.path.basename(filename))
		outFileName  = addFileNameAppendage(outFileName, args.odix)
		outFileName  = createOutputFileName(outFileName)
		outFilePtr = open(outFileName, 'wb')
		print ("writing to file: %s" % outFileName)

		# d = []
		# t = []
		attitudeData = []
		# read the data from a file.
		with open(filename, 'r') as csvfile:
				reader = csv.reader(csvfile, delimiter=',', quotechar='|')
				header = next(reader) 
				for row in reader:
					timestamp = float(row[0])
					roll	= float(row[1])
					pitch	= float(row[2])
					heave	= float(row[3])
					heading = float(row[4])
					if len(row) > 4:
						height = float(row[5])
						attitudeData.append([timestamp, roll, pitch, heave, heading, height])
					else:
						attitudeData.append([timestamp, roll, pitch, heave, heading, 0.000])

		filteredAttitudeData = smoothBySavGol(attitudeData, level)
		
		# create some nice plots for the 
		createPlots(filename, outFileName, attitudeData, filteredAttitudeData, False)

		# write the results to a CSV
		with open(outFileName,'w') as file:
			for h in header:
				file.write(h + ",")
			file.write("\n")
			for rec in filteredAttitudeData:
				# save the corrected data back to a CSV file in the same format as it was read.
				line = ("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n" % (rec[0], rec[1], rec[2], rec[3], rec[4], rec[5]))
				file.write(line)


def createPlots(filename, outFileName, attitudeData, filteredAttitudeData, plotHeight=False):
		arr = np.array(attitudeData)
		ts = arr[:,0]
		rawroll = arr[:,1]
		rawpitch = arr[:,2]
		rawHeave = arr[:,3]
		rawHeight = arr[:,5]
		
		arr = np.array(filteredAttitudeData)
		correctedHeave = arr[:,3]
		smoothedHeight = arr[:,5]

		smoothedHeave = arr[:,6]

		plt.figure(figsize=(12,4))
		# plt.axhline(0, color='black', linewidth=0.3)
		plt.grid(linestyle='-', linewidth='0.2', color='black')

		raw = plt.plot(rawHeave, color='red', linewidth=0.2, label='Raw Heave')
		error = plt.plot(correctedHeave, color='green', linewidth=0.5, label='Corrected Heave')
		error = plt.plot(smoothedHeave, color='red', linewidth=0.2, label='Smoothed Heave')

		if plotHeight:
			ht = plt.plot(rawHeight, color='blue', linewidth=0.2, label='Raw Height')
			ht = plt.plot(smoothedHeight, color='green', linewidth=0.5, label='Smoothed Height')

		plt.legend()
		plt.xlabel('Sample #')
		plt.ylabel('Heave(m)')
		plt.title(os.path.basename(filename))
		plt.savefig(os.path.splitext(outFileName)[0]+'.png', dpi = 300)
		# plt.show()

def smoothBySavGol(attitudeData, level=1000):
	# move this to the savgol function so it can be called externally
	ts = []
	roll = []
	pitch = []
	rawHeave = []
	heading = []
	rawHeight = []

	for row in attitudeData:
		ts.append(float(row[0]))
		roll.append(float(row[1]))
		pitch.append(float(row[2]))
		rawHeave.append(float(row[3]))
		heading.append(float(row[4]))
		rawHeight.append(float(row[5]))

	# isolate the low frequency signal in the heave(which should not exist)
	smoothedHeave = signal.savgol_filter(rawHeave, 101, 1) 
	for i in range(level):
		smoothedHeave = signal.savgol_filter(smoothedHeave, 101, 1) 
	# subtract the very smoothed signal from the input signal, thereby applying a lowcut filter (AKA high band pass)
	settledHeave = np.subtract(rawHeave, smoothedHeave)
	
	# pkpk
	# settledHeave = smoothed
		
	# now remove the DC offset in the heights.
	# DC = np.mean(rawHeight)
	# levelHeight = rawHeight - DC
	# subtract the smoothed heave from the height
	# smoothedHeight= np.subtract(levelHeight, -smoothed)
	
	# remove the short period heave...
	# smoothedHeight= np.subtract(levelHeight, - settledHeave)
	
	# we can see the heave signal is in the Height records, so remove it here. This was confirmed by Dylan.
	# smoothedHeight= rawHeight
	smoothedHeight= np.subtract(rawHeight, - settledHeave)
	smoothedHeight = signal.savgol_filter(smoothedHeight, 101, 1) 
	for i in range(level):
		smoothedHeight = signal.savgol_filter(smoothedHeight, 101, 1) 

	# https://stackoverflow.com/questions/47484899/moving-average-produces-array-of-different-length
	# y_padded = np.pad(smoothedHeight, (level//2, level-1-level//2), mode='edge')
	# smoothedHeight = np.convolve(y_padded, np.ones((level,))/level, mode='valid') 

	filteredAttitudeData = []
	for j in range(0, len(ts)):
		# save the corrected data back to a CSV file in the same format as it was read.
		filteredAttitudeData.append([ts[j], roll[j], pitch[j], settledHeave[j], heading[j], smoothedHeight[j], smoothedHeave[j]])

	return filteredAttitudeData
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


if __name__ == "__main__":
        main()
