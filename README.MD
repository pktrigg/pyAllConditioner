pyAllConditioner
=====
* created:          May 2018
* version           2.0
* by:               paul.kennedy@guardiangeomatics.com
* description:      python module to read and write a Kongsberg .ALL file making subtle modifications along the way
* notes:            See main at end of script for example how to use this
* based on ALL file version October 2013 
* developed for Python version 3.4

2DO
===
beamqc: Dx or raw range datagrams? test to ensure they are identical
beamqc: write out results to csv
beamqc: read and apply the results to the f datagrams
beamqc: encode for D datagrams so we can reject or change the depths.  it looks like caris reads quality from D records.

implement -splitsize
implement -splitduration

Done
====
sort out checksums in encoders.  It now reads fine in sonar viewer
add ENCODE for 'f' records
added decode for 'f' datagrams (EM3002 raw ranges)
added -odix so user can add an appendage to the filename
injectAH attitude and height now implemented.  Height records are limited to sensible rates as they are only used for tides.
reworked extraction file format.  It is now unixtime instead of 2 strings for data and time.  Much easier to deal with!
added -attitudeHeight to extract height datagram and interpolate it into the attitude CSV file, so we can inject later.
added -height to extract height datagram

trap overflows in backscatter samples due to short integer
Backscatter conditioning:
    load a corrections file into memory which contains a list of sectornumber, correction(dB) values
    read the raw rang datagram and create a mean to sector lookup
    read the seabedimage datagram
    for each beam in the seabedimage daagram, find the sector, find the correction and apply it to the seabedimage datagram
    write out the new datagram by modifying the raw bytes 
    added support for '-bscorr' to export the 3_EXTRA_PARAMETERS datagram into a bscorr file (if present)
implement -splitdepthmode
    we need to store the installation records start and end, 
    We then start spooling the datagrams into s file which will have a sensible name eg <filename>_mode.all.  
    We then close the file each time there is a change in the depth mode within the runtime datagrams, and open a new file
backscatter ARC now saves the sum and count, so users can understand how reliable the resulting ARC is.
backscatter ARC now computes a correction column so the apply option in this tool becomes useful
SVP filename is now more ligical and imports multiple SVP's if present in .all file.
Implemented extraction of SVP so we can easily consume the profile in CARIS
Implement -inject <filename> to inject .srh delayed heave into the .all file.
    read file for injection into memory with an essential time field
    loop through input file
        if input current datagram time is older than the top of the injection file, write out the raw bytes
        if the top of the injection file is older than the current datagram, write out the injection record, and pop it from the list
    close the input file
    close the newly created file
added support for odir so we can keep the output in a different folder.
encode for X records.
SRH needs to import multiple SRH files and sort them into a sensible order
use glob to permit multiple SRH files to be imported for injection using a sorted wildcard
SRH decode is now operational, and tested against Kongsberg sample data
iterates through the Y seabedImage datagrams and writes out an angular response curve file in the format angle,response,sector
the ARC is a sum of ALL input files and can be used as the basis to condition and improve (ie correct) the Y datagrams by applying a simplar CSV file in the format angle, response, sector, correction.  The correction fields are added to the y datagrams
Improve wildcard and single file support
Implement multiple record attitude injection
Implement attitude injection
Implement writing of the new file.  by default use append _1.all to make a new filename
Implement -x <recordIdentifiers> to exclude records from the output, eg -x PA to remove the position (P) and attitude (A) records.  Note this needs to be case sensitive

Examples
-------

To see the command line help...
```
python pyAllConditioner.py
```
To exclude 'A' attitude datagrams .all file...
```
python pyallconditioner.py -i <filename.all> -exclude A
```
To strip non-essential records from .all file for pushing back2base
```
python pyallconditioner.py -i <filename.all> -exclude nYNC0
Note: this will remove the 'n' network attitude, 'Y' seabed imagery, 'N' raw range, 'C' clock, '0' extra datagrams.  In an example .all file, this reduced the file size from 418Mb to 126Mb. You can then zip that up into 86Mb for shipping.  
```

To EXTRACT SVP profiles from the .all file into an ASCII format which can be imported directly into CARIS
```
python pyallconditioner.py -i <filename.all> -svp

The resulting svp file will look like this...
[SVP_Version_2]
d:\projects\RVInvestigator\0073_20161001_103120_Investigator_em710.all
Section 2016-274 10:31:20 -21:58:7.988 151:25:33.901
0.000 1534.600 
4.970 1534.600 
5.960 1534.600 
6.960 1534.600 
etc
The date and position will be extracted from the .all file, which makes this very easy to manage in CARIS
```

To SPLIT FILE WHEN DEPTH MODES CHANGES, 
```
python pyallconditioner.py -i <filename.all> -splitd

This tool read teh R_RUNTIME records and each time the depth mode changes, it closes the conditioned file, thereby splitting based on depoth mode.  for convenicnce, the file name is appended with the depth mode.  This is very useful for analysing the backscatter as we can analyse each mode independently.
```


To EXTRACT ANGULAR RESPONSE CURVES from the 'Y' seabed image datagrams, 
```
python pyallconditioner.py -i <filename.all> -extractbs

This will create an ASCII CSV file with a 1 degree resolution of angle, ampliture response and transmit sector.  The amplitudes represent the average amplitude for all samples in that angle range (+/-0.5degree).  Take this file and plot it in excel to reveal how well balanaced the varius transmit sectors are.  Using excel it is then very easy to create an additional 'correction' column which can be applied to the 'Y' datagrams using the -conditionbs option within this tool.

```

To INJECT .srh files (seapath real heave)
```
python pyallconditioner.py -i <filename.all> -srh "*.srh"
This option will read one or multiple srh files, sort them into ascending data order, import them and inject them into a new .all file in the correct locations (interleaving) by inserting a 'A' attitude datagram with a system #2 identifier, descriptor of not_in_use and zero for the roll, pitch and heading fields.  
This option is very useful for ensuring the .all files are self-contained (everything you need is in the 1 spot), durable (less chance of losing auxiliary data files), portable (it is much easier to send 1 file than forget to send all files used in processing) and open (the .all file format) is well documented and understood by many applications.
If needed, the existing attitude records can be excluded from the process, but under normal circumstances this is not recommended.

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