"""
http://stackoverflow.com/questions/13718821/run-python-script-as-daemon-at-boot-time-ubuntu


This script is the main file to control jobs and run commands to move data. This is executed as a daemon.

check jobs from ecmwf
if finished -> check remote outputs (8 expected)
            -> download data 

"""

import os
import abc
import json
import pickle
import socket
import logging as log

from ftplib import FTP, error_perm
from automatization.ecaccess import EcmwfJob, Ecaccess

log.basicConfig(level=log.INFO)


class EcmwfInput(object):
    """
    In order to move data it is necessary to specify configuration first
    """
           
    def __init__(self, _name, _description, _remote_path, _local_path):
        """
        Constructor
        
        Arguments
        ---------
        _name: str
            configuration name
        _description: str
            configuration description
        _remote_path: str
            source remote path to copy data    
        _local_path: str
            destination local path
        """
        if not isinstance(_name, str):
            raise Exception("ftpPath is not str")        
            
        if not isinstance(_description, str):
            raise Exception("outputPath is not str")        
    
        if not _remote_path or not isinstance(_remote_path, str):
            raise Exception("FtpPath is not defined")
            
        if not _local_path or not isinstance(_local_path, str):
            raise Exception("OutputPath is not defined")
        
        self.name = _name
        self.description = _description
        self.remote_path = _remote_path
        self.local_path = _local_path
                
    #@abc.abstractmethod
    def getRemotePath(self):
        return self.remote_path
    
    #@abc.abstractmethod
    def getLocalPath(self, dateobj):
        """
        Generate output path as it is dynamic.
        
        Arguments
        ---------
        dateobj: date.date
            Date 
        
        Return
        ------
        newpath: str
            This path is generated
        
        """
        newpath = self.local_path.replace("YEAR", dateobj.strftime("%Y"))
        newpath = newpath.replace("MONTH", dateobj.strftime("%m"))
        newpath = newpath.replace("DAY", dateobj.strftime("%d"))
        
        return newpath
 
###################################################################################
#
#       UTILS method
#
#
###################################################################################


def split_filepath(filepath):
    """
    Extract filename and filepath from a complete path
    
    Arguments
    ---------
    filepath: str
        complete file path 
            
    Return
    ------
    filename: str
        file name                
    filepath: str
        path to file name
    """
    fullpath_list = filepath.split("/")[:-1]
    fullpath = "/".join(fullpath_list)
    filename = filepath.split("/")[-1]

    return filename, fullpath
 
###################################################################################
 
 
class FTPManager(object):
    """
    FTP basic method to connect to ftp    
    """
    def __init__(self, _address, _user, _password):
    
        if not _user:
            raise Exception("user is not defined")
            
        if not _password:    
            raise Exception("user is not defined")
    
        self.address = _address
        self.user = _user
        self.pwd = _password
        
    def list_files(self, path):
        """
        Get ftp listed files
        
        Arguments
        ---------
        path: str
            remote path to list ftp files
            
        Return
        ------
        files: list of str
            return a list of all files found. Empty if nothing found
            
        """
        if not path:
            raise Exception("path is not defined")    
            
        files = None    
    
        # connect to ic3 ftp and retrieve files
        with FTP(host=self.address) as ftp:
            ftp.login(user=self.user,passwd=self.pwd)
            ftp.cwd(path)

            files = ftp.nlst()
            
        return files  
            
    def download(self, remote_filepath, filepointer):
        """
        Get data from ftp to local machine
        
        Arguments
        ---------
        remote_filepath: str
            Complete ftp path and filename
        filepointer: file object
            This is a local file pointers to place downloaded data
        """            
        print(remote_filepath)
        remote_filename, remote_path = split_filepath(remote_filepath)
        print (remote_filename, remote_path)
                        
        with FTP(host=self.address) as ftp:
            ftp.login(user=self.user,passwd=self.pwd)
            ftp.cwd(remote_path)
            ftp.retrbinary('RETR %s' % remote_filename, filepointer.write)
                
    def delete(self, remote_filepath):
        """
        Delete file on ftp
        
        Arguments
        ---------
        remote_filepath: str
            ftp full path to file
        """    
        remote_filename, remote_path = split_filepath(remote_filepath)
        
        # connect to ic3 ftp
        with FTP(host=self.address) as ftp:
            ftp.login(user=self.user,passwd=self.pwd)
            ftp.cwd(remote_path)
            ftp.delete(remote_filename)


class DataManager(object):    
    """
    Manage data importing from Remote site to Local machine
    """
    
    def __init__(self, _ftpManager, _inputData):
        """
        constructor
        
        Arguments
        ---------
        _ftpManager: object FTPManager
            Remote connection
        _inputData: object EcmwfData
            Configuration Parameters for local and remote places
        """
        
        self.input = _inputData
        self.ftp = _ftpManager
            
    def getJobs(self):
        """
        Discover on ftp how many jobs have finished regarding the output files.
        
        Return
        ------
        done_jobs: list of EcmwfJob
            successfully finished job 
        
        exec_jobs: list of EcmwfJob
            running jobs. All outputs are not yet found on ftp.
        """
        succ_jobs = []
        run_jobs = []
        ending = ["00","03","06","09","12","15", "18","21"]
        
        # connect to ic3 ftp and retrieve files
        files = self.ftp.list_files(self.input.getRemotePath())
               
        # dates and files found
        dates_found = {}    
            
        # organize each file found
        for f in files:
            if f.endswith(".tmp"):
                log.debug("Skipping file, this is a temporary file: (%s)", f)
                continue

            datestr = f[2:-2]
            
            if not datestr in dates_found:
                dates_found[datestr] = []
            
            dates_found[datestr].append(f[-2:])
        
        # delete repeated elements        
        for key, value in dates_found.items():
            dates_found[key] = set(dates_found[key])
            
        # searching for all expecting files for this date
        for key, ends_found in dates_found.items():
        
            # count how many files related to date
            ends_c = 0        
            for expected_end in ending:            
                # this should not happend
                if not expected_end in dates_found[key]:
                    # this should not happen, error
                    #print("Warning: not expected file output %s" % expected_end)
                    pass
                    #break
                else:    
                    ends_c += 1
            
            # include if the same number    
            if ends_c == len(ending):
                succ_jobs.append(key)
            # less files found than expected (not finished yet)    
            elif ends_c < len(ending):
                # add only if not exists
                if not key in run_jobs:
                    run_jobs.append(key)    
            else:
                raise Exception("Output file not expected to exists")        
            
        # create ecmwf done jobs        
        done_jobs = []
        for x in succ_jobs:
            job = EcmwfJob.from_date(x)
            done_jobs.append(job)
        
        # create ecmwf exec jobs    
        exec_jobs = []
        for x in run_jobs:
            job = EcmwfJob.from_date(x)
            job.job_status = "EXEC"
            exec_jobs.append(job)
            
        return done_jobs, exec_jobs
            
    def _download_ftp_data(self, filenames, download_path):
        """
        Download data from ic3 ftp server and place it
        in climadat nas in the right folder.
        
        Arguments
        ---------
        filenames: list of str
            complete full path with files to download
        download_path: str
            local full path to place download    

        Return
        ------
        downloaded_files: list of str
            successfully download files
        """
        if not filenames:
            raise Exception("Specify a list of names")

        # files download
        downloaded_files = []

        # there is a set of files
        for filename in filenames:
            log.debug("  Download %s..." % (filename))
            
            # create intermediate folders if not exists
            if not os.path.exists(os.path.dirname(download_path)):
                os.makedirs(os.path.dirname(download_path))            
                log.debug("  Folder created %s" % download_path)

            # download and write file to disk
            local_filepath = download_path + filename
            with open(local_filepath, 'wb') as outfile:
                log.debug("  Writing to %s..." % (local_filepath))
                outfile = open(local_filepath, 'wb')
                remote_filepath = self.input.getRemotePath() + "/" + filename
                #log.debug("%s %s" % (remote_filepath, outfile))
                self.ftp.download(remote_filepath, outfile)
                
                downloaded_files.append(filename)

        return downloaded_files      
        
    def _delete_ftp_data(self, filenames):
        """
        Delete filenames from FTP
        
        Arguments
        ---------
        filenames: list of str
            full path to remote files to delete
        """
        if not filenames:
            raise Exception("Specify a list of names")

        for filename in filenames:            
            remote_path = self.input.getRemotePath() + "/" + filename
            self.ftp.delete(remote_path)
       
    def download(self, is_simulation=False):
        """        
        Main method to check jobs
        -check ecmwf jobs
        -download data to folder
        -remove data from ftp
        
        Arguments
        ---------
        is_simulation: bool
            Apply action but with no effect on real data. Test purposes.
        """

        log.info("Analyzing FTP outputs...")
        try:
            finished_jobs, exec_jobs = self.getJobs()
        except socket.gaierror:
            log.debug("Internet/local network is down. Skipping check...")
            return
        
        log.info("---------------------------")
        log.info("")
        log.info("  Summary for %s:" % self.input.name)
        log.info("    Completed jobs: %d" % len(finished_jobs))    
        log.info("    Running jobs: %d" % len(exec_jobs))
        log.info("")
        log.info("---------------------------")

        log.debug(os.getcwd())

        print("Checking completed jobs...")
        for job in finished_jobs:    
            pattern = self.input.getRemotePath() + "/EN" + job.simulation_date.strftime("%y%m%d") + "*"
            
            climanas_path = self.input.getLocalPath(job.simulation_date)
            
            log.info(" Downloading from remote %s to %s ..." % (pattern, climanas_path))        
            try:
                if not is_simulation:
                    self._download_ftp_data(job.get_outputs_filenames(), climanas_path)
            except error_perm:
                log.debug("Error downloading file, skipping for next chance...")
                lob.debug(traceback.print_exc())
                break
       
            log.debug(" Deleting files from FTP...")
            if not is_simulation:
                self._delete_ftp_data(job.get_outputs_filenames())   
        
    
