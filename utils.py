# Databricks notebook source
import subprocess
import os
from threading import Timer
import sys
import logging.config
from typing import Any
from typing import Optional
import json

# COMMAND ----------

def run_cmd(command: list):
    # Execute the Bash script using subprocess
    # process = subprocess.Popen(['bash', '-c', command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Read and print the output and error
    while True:
        output = process.stdout.readline()
        error = process.stderr.readline()
        
        if output == '' and error == '' and process.poll() is not None:
            break
        
        if output:
            print(output, end='')
        
        if error:
            print(error, end='')

    # Wait for the process to finish
    process.wait()
    return process

# COMMAND ----------


