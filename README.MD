pyAllConditioner
=====
* created:          November 2016
* version           1.0
* by:               p.kennedy@fugro.com
* description:      python module to read and write a Kongsberg .ALL file making modifications along the way
* notes:            See main at end of script for example how to use this
* based on ALL file version October 2013 
* developed for Python version 3.4 + 2.7

2DO
===
Implement -inject <filename> to inject .srh delayed heave into the .all file.

Done
====
Implement writing of the new file.  by default use append _1.all to make a new filename
Implement -x <recordIdentifiers> to exclude records from the output, eg -x PA to remove the position (P) and attitude (A) records.  Note this needs to be case sensitive

Example
-------

To see the command line help...
```
python pyAllConditioner.py
```
To create a strip attitude records a .all file...
```
python pyall2shp.py -i <filename.all> -x A
```

output
------
```
A new .all file with modified as per the command line switches
```