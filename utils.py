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

def run_cmd(cmd,
            throw_on_error=True,
            env=None,
            stream_output=False,
            timeout_seconds=0,
            log_command_on_error=True,
            **kwargs):
    """Runs a command as a child process.

    A convenience wrapper for running a command from a Python script.
    Keyword arguments:
    cmd -- the command to run, as a list of strings
    throw_on_error -- if true, raises an Exception if the exit code of the program is nonzero
    log_command_on_error -- if true, prints the command when there is an error
    env -- additional environment variables to be defined when running the child process
    stream_output -- if true, does not capture standard output and error; if false, captures these
      streams and returns them

    Note on the return value: If stream_output is true, then only the exit code is returned. If
    stream_output is false, then a tuple of the exit code, standard output and standard error is
    returned.
    """
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)
    # HACK: If we are invoking a command that's not inside our runfiles directory,
    # make sure we don't propagate RUNFILES_DIR to the child process. This is necessary because
    # we often build undeclared dependencies via `bazel build` here and they should have their own
    # runfiles instead of inheriting ours.
    if ".runfiles" not in cmd[0]:
        cmd_env.pop("RUNFILES_DIR", None)

    timer = None
    kill = lambda process: process.kill()

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, universal_newlines=True, **kwargs)

        if timeout_seconds:
            timer = Timer(timeout_seconds, kill, [child])
        exit_code = -1
        try:
            if timer:
                timer.start()
            exit_code = child.wait()
        finally:
            if timer:
                timer.cancel()

        if throw_on_error and exit_code != 0:
            if log_command_on_error:
                raise Exception("Non-zero exitcode %s for command %s" % (exit_code, cmd))
            else:
                raise Exception("Non-zero exitcode %s for command REDACTED" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            **kwargs)

        if timeout_seconds:
            timer = Timer(timeout_seconds, kill, [child])
        stdout, stderr, exit_code = None, None, -1
        try:
            if timer:
                timer.start()
            (stdout, stderr) = child.communicate()
            exit_code = child.wait()
        finally:
            if timer:
                timer.cancel()

        if throw_on_error and exit_code != 0:
            if log_command_on_error:
                raise Exception("Non-zero exitcode %s for command %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                                (exit_code, cmd, stdout, stderr))
            else:
                raise Exception(
                    "Non-zero exitcode %s for command REDACTED\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                    (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)

# COMMAND ----------


