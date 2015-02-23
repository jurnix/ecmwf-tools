ecmwf
=====

This tools are created in order to downlaod data from an FTP server.

It expects to find 8 files with the following format:

-ENYYMMDD00
-ENYYMMDD03
-ENYYMMDD06
-ENYYMMDD09
-ENYYMMDD12
-ENYYMMDD15
-ENYYMMDD18
-ENYYMMDD21

If those conditions are satisfied it starts the transfer.


Example
=======

```
from automatization.datamanager import FTPManager, EcmwfInput, DataManager

 def check_jobs():
    # define ftp site
    ftp = FTPManager("ftp.site.com", "user", "password")

    # sites to manage
    # Name
    # Description
    # ftp remote path
    # local path -date params are replaced by current file date-
    sitesArgs = EcmwfInput("climasites","input data for climasites","/remote_path","/some/local/path/YEAR/MONTH/DAY/")

    dmsites = DataManager(ftp, sitesArgs)
    # check for file and download
    dmsites.download()

if __name__ == "__main__":
    check_jobs()
```
