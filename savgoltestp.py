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
		outFileName  = createOutputFileName(outFileName)
		outFilePtr = open(outFileName, 'wb')
		print ("writing to file: %s" % outFileName)

		d = []
		t = []
		roll = []
		pitch = []
		rawHeave = []
		heading = []
		with open(filename, 'r') as csvfile:
				reader = csv.reader(csvfile, delimiter=',', quotechar='|')
				header = next(reader) 
				for row in reader:
					d.append(float(row[0]))
					t.append(float(row[1]))
					roll.append(float(row[2]))
					pitch.append(float(row[3]))
					rawHeave.append(float(row[4]))
					heading.append(float(row[5]))

		smoothed = signal.savgol_filter(rawHeave, 101, 1) 
		for i in range(level):
			smoothed = signal.savgol_filter(smoothed, 101, 1) 

		diff = np.subtract(rawHeave, smoothed)

		with open(outFileName,'w') as file:
			for h in header:
				file.write(h + ",")
			file.write("\n")
			for j in range(0, len(d)):
				# save the corrected data back to a CSV file in the same format as it was read.
				line = ("%d,%.3f,%.3f,%.3f,%.3f,%.3f\n" % (d[j], t[j], roll[j], pitch[j], diff[j], heading[j]))
				file.write(line)

		plt.figure(figsize=(12,4))
		plt.axhline(0, color='black', linewidth=0.3)
		plt.grid(linestyle='-', linewidth='0.2', color='black')

		raw = plt.plot(rawHeave, color='red', linewidth=0.2, label='Raw Heave')
		error = plt.plot(smoothed, color='black', linewidth=0.2, label='Heave Error')
		corr = plt.plot(diff, color='blue', linewidth=0.2, label='Corrected Heave')

		plt.legend()
		plt.xlabel('Sample #')
		plt.ylabel('Heave(m)')
		plt.title(os.path.basename(filename))
		plt.savefig(os.path.splitext(outFileName)[0]+'.png', dpi = 300)
		# plt.show()

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

if __name__ == "__main__":
        main()
