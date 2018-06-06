import csv
import sys
import time
import math
import os
import fnmatch
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from glob import glob
from shutil import copyfile
from sys import exit

###############################################################################
def main():
	parser = ArgumentParser(description='Search for files in subfolders and copy to -odir folder maintaining the parent name.',
			epilog='Example: \n -r -i /carisproject/*.all -odir /raw \n to copy every .all file from caris project into the /raw folder\n', formatter_class=RawTextHelpFormatter)
	parser.add_argument('-i', dest='inputFile', action='store', help='Input filename. It can also be a wildcard, e.g. *.all')
	parser.add_argument('-odir', dest='odir', action='store', default="", help='Specify a relative output folder e.g. -odir raw')
	parser.add_argument('-r', action='store_true', default=False, dest='recursive', help='Search Recursively from the current folder.  [Default: False]')

	if len(sys.argv)==1:
		parser.print_help()
		sys.exit(1)
		
	args = parser.parse_args()

	matches = []

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

	# print (matches)

	for filename in matches:
		parent = os.path.dirname(os.path.dirname(filename))
		# newname = os.path.dirname(filename)
		newname = os.path.split(os.path.dirname(filename))[-1] + ".all"
		newname = os.path.join(args.odir, parent, newname)
		print ("copying:" + filename + " to:" + newname)
		if not os.path.exists(os.path.dirname(newname)):
			os.makedirs(os.path.dirname(newname))
		copyfile(filename, newname)

###############################################################################
if __name__ == "__main__":
	start_time = time.time() # time  the process
	main()
	print("Duration: %d seconds" % (time.time() - start_time))
