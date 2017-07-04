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
SRH needs to import multiple SRH files and sort them into a sensible order

Implement -inject <filename> to inject .srh delayed heave into the .all file.
    read file for injection into memory with an essential time field
    loop through input file
        if input current datagram time is older than the top of the injection file, write out the raw bytes
        if the top of the injection file is older than the current datagram, write out the injection record, and pop it from the list
    close the input file
    close the newly created file

Backscatter conditioning:
    load a corrections file into memory which contains a list of sectornumber, correction(dB) values
    read the raw rang datagram and create a mean to sector lookup
    read the seabedimage datagram
    for each beam in the seabedimage daagram, find the sector, find the correction and apply it to the seabedimage datagram
    write out the new datagram by modifying the raw bytes 
Done
====
use glob to permit multiple SRH files to be imported for injection using a sorted wildcard
SRH decode is now operational, and tested against Kongsberg sample data
iterates through the Y seabedImage datagrams and writes out an angular response curve file in the format angle,response,sector
the ARC is a sum of ALL input files and can be used as the basis to condition and improve (ie correct) the Y datagrams by applying a simplar CSV file in the format angle, response, sector, correction.  The correction fields are added to the y datagrams
Improve wildcard and single file support
Implement multiple record attitude injection
Implement attitude injection
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

Data Type Conversions from ALL to Python::     
    * ALL types to python struct types
    * signed char = 1 byte = "b"
    * UNSIGNED char = 1 byte = "B"
    * XTFWORD = signed int 2 bytes = h
    * XTFWORD = UNSIGNED int 2 bytes = H (for unipolar data)
    * short = short integer 2 bytes = "h"
    * DWORD = unsigned int 4 bytes = "L"
    * char = 1 byte = "c"
```