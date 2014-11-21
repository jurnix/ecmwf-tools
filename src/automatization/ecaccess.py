"""

"""

import subprocess

from datetime import datetime, date


class Ecaccess(object):
    """
    ecacccess interface to ecaccess package
    """

    def get_list_jobs(self):
        """
        ecaccess-job-list
        """
        # get jobs
        bashCommand = "ecaccess-job-list"
        jobs_list = []

        # run command line
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output_byte = process.communicate()[0]

        output_str = output_byte.decode("utf-8")
        
        # process output
        for line in output_str.split('\n'):
            splitted = line.split(" ")
            params = [i for i in splitted if i != '']
            # debug purpose
            # print(params)

            # empty line    
            if not params:
                continue

            new_job = EcmwfJob(params)
            jobs_list.append(new_job)
            #print(new_job)
            #print("")

        # print(jobs_list)
        return jobs_list

    def submit_job(self, script_name):
        """
        ecaccess-job-submit script_name

        return: assigned job id
        """
        # get jobs
        bashCommand = "ecaccess-job-submit %s" % (script_name)

        # run command line
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output_byte = process.communicate()[0]

        job_id = output_byte.decode("utf-8")
        return int(job_id)


    def delete_job(self, job_id):
        """
        ecaccess-job-delete script_name

        return: assigned job id
        """
        # get jobs
        bashCommand = "ecaccess-job-delete %s" % (job_id)

        # run command line
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        process.communicate()[0]

        
    def files_list(self, pattern):
        """
        List ecmwf files

        ecaccess-file-dir
        """
        if not pattern:
            raise Exception("A folder is required")

        # get jobs
        bashCommand = "ecaccess-file-dir %s" % (pattern)

        # run command line
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output_byte = process.communicate()[0]

        files_raw = output_byte.decode("utf-8")
        files = files_raw.split('\n')
        # removing empty positions
        clean = [i for i in files if i != '']
        return clean

    def download_data(self, remote_source, destination):
        """
        Data to download
        """
        # get jobs
        bashCommand = "yes | ecaccess-file-mget %s %s" % (remote_source, destination)
        print("DEBUG: %s" % bashCommand)

        # run command line
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        process.communicate()[0]

        

class EcmwfJob(object):
    """
    This class is a python interface to ecaccess tools
    from ecmwf.
    """
    jobs_status={
    "INIT": "Jobs are being initialised",
    "STDBY": "Jobs are waiting for an event",
    "EXEC": "Jobs are running",
    "WAIT": "Jobs have been queued to the scheduler (e.g. LoadLeveler)",
    "RETR": "Jobs will be resubmitted",
    "STOP": "Jobs have NOT completed (error)",
    "DONE": "Jobs have successfully completed"}

    def __init__(self, _job_id=None, _gateway=None, _job_status="DONE", _scriptName=None, _date=None, _simulation_date=None, _run_number=None):
        """
        """
        if _date and not isinstance(_date, datetime):
            raise Exception("Experiment's date must be datetime type")
            
        if _simulation_date and not isinstance(_simulation_date, date):
            raise Exception("Simulation date must be date type")
    
        if _job_status and not _job_status in self.jobs_status.keys():
            raise Exception("Job status must be included in: %s" % self.jobs_status)
    
        self.job_id = _job_id
        self.gateway = _gateway
        self.job_status = _job_status
        self.script_name = _scriptName
        self.date = _date
        self.simulation_date = _simulation_date
        self.run_number = _run_number       
        
    @classmethod
    def from_vector(cls, vect):        
        """
        Initialize Ecmwf job parsing info from ecaccess-job-list
        
        input array with params:
        job id
        gateway
        job status,
        run number
        month
        day
        time
        unknown,
        script name
        """
        if len(toParse) is not 9:
            raise Exception("9 Params expected")

        job_id = int(toParse[0])
        gateway = toParse[1]
        job_status = toParse[2]
        run_number = toParse[3]

        # include current year
        str_date = toParse[4:7] + ["2014"]
        date = datetime.strptime(' '.join(str_date), "%b %d %H:%M %Y" )

        script_name = toParse[8]

        # convert filename to date in string (e.g (SOMETHING_local_20140201.sh) to 20140201)
        str_date = script_name.split("_")[2].split(".")[0]
        # set to date object
                
        print(str_date)
        simulation_date = date(int(str_date[0:4]), int(str_date[4:6]), int(str_date[6:8]))
        return cls(_job_id = job_id, _gateway = gateway, _job_status = job_status, _run_number=run_number, _date = date,
             _simulation_date=simulation_date, _scriptName=script_name)
        
    @classmethod
    def from_date(cls, str_date):
        """
        Initialize EcmJob with only its generated outputs
        """        
        job_status = "DONE"
        date = datetime.strptime(str_date, "%y%m%d")
        simulation_date = date.strptime(str_date, "%y%m%d")
        
        return cls(_job_status = job_status, _date = date, _simulation_date=simulation_date)   

    def __repr__(self):
        output="Job id: %s\n" % str(self.job_id)
        output += "Gateway name: %s\n" % self.gateway
        output += "Job status: %s\n" % self.job_status
        output += "Run number: %s\n" % self.run_number
        output += "Submission date: %s\n" % self.date
        output += "Script name: %s" % self.script_name
        return output

    def __str__(self):
        output="Job id: %s\n" % str(self.job_id)
        output += "Gateway name: %s\n" % self.gateway
        output += "Job status: %s\n" % self.job_status
        output += "Run number: %s\n" % self.run_number
        output += "Submission date: %s\n" % self.date
        output += "Script name: %s\n" % self.script_name
        output += "outputs: %s" % self.get_outputs_filenames()

        return output        

    def get_outputs_filenames(self):
        """
        return a list with those expected file names
        """
        output=[]
        steps=["00","03","06","09","12","15","18","21"]
        name = self.date.strftime("%y%m%d")
        
        for step in steps:
            output.append("EN%s%s" % (name, step))

        return output
