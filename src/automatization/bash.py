import os
import subprocess

def run_bash_cmd(path, bashCommand):
    """
    Run bash command
    """

    # move to directory
    if path:
        print("Moving to %s..." % path)
        os.chdir(path)
    # bashCommand = "./bucle_flexpart.sh 20140510 1 8"

    print("Running %s..." % bashCommand)
    # run command line
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output_byte = process.communicate()[0]

    output_str = output_byte.decode("utf-8")

    return output_str
