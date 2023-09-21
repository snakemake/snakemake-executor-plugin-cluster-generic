__author__ = "Johannes Köster, Manuel Holtgrewe"
__copyright__ = "Copyright 2023, Johannes Köster, Manuel Holtgrewe"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from dataclasses import dataclass, field
import os
import shlex
import subprocess
import sys
import threading
import time
from typing import Generator, List, Optional

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)


# Optional:
# define additional settings for your executor
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    submit_cmd: Optional[str] = field(
        default=None,
        metadata={
            "help": "Command for submitting jobs",
            "required": True,
        },
    )
    status_cmd: Optional[str] = field(
        default=None, metadata={"help": "Command for retrieving job status"}
    )
    cancel_cmd: Optional[str] = field(
        default=None,
        metadata={
            "help": "Command for cancelling jobs. Expected to take one or more "
            "jobids as arguments."
        },
    )
    cancel_nargs: int = field(
        default=20,
        metadata={
            "help": "Number of jobids to pass to cancel_cmd. If more are given, "
            "cancel_cmd will be called multiple times."
        },
    )
    sidecar_cmd: Optional[str] = field(
        default=None, metadata={"help": "Command for sidecar process."}
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=False,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __init__(
        self,
        workflow: WorkflowExecutorInterface,
        logger: LoggerExecutorInterface,
    ):
        super().__init__(
            workflow,
            logger,
            # configure behavior of RemoteExecutor below
            # whether arguments for setting the remote provider shall  be passed to jobs
            pass_default_remote_provider_args=True,
            # whether arguments for setting default resources shall be passed to jobs
            pass_default_resources_args=True,
            # whether environment variables shall be passed to jobs
            pass_envvar_declarations_to_cmd=True,
        )

        if self.workflow.executor_settings.submit_cmd is None:
            raise WorkflowError(
                "You have to specify a submit command via --cluster-generic-submit-cmd."
            )

        self.sidecar_vars = None
        if self.workflow.executor_settings.sidecar_cmd:
            self._launch_sidecar()

        if (
            not self.workflow.executor_settings.status_cmd
            and not self.workflow.storage_settings.assume_shared_fs
        ):
            raise WorkflowError(
                "If no shared filesystem is used, you have to "
                "specify a cluster status command."
            )

        self.status_cmd_kills = []
        self.external_jobid = dict()

    def run_job(self, job: JobExecutorInterface):
        jobscript = self.get_jobscript(job)
        self.write_jobscript(job, jobscript)

        jobfinished = self.get_jobfinished_marker(job)
        jobfailed = self.get_jobfailed_marker(job)

        job_info = SubmittedJobInfo(
            job,
            aux={
                "jobscript": jobscript,
                "jobfinished": jobfinished,
                "jobfailed": jobfailed,
            },
        )

        if self.workflow.executor_settings.status_cmd:
            ext_jobid = self.dag.incomplete_external_jobid(job)
            if ext_jobid:
                # Job is incomplete and still running.
                # We simply register it and wait for completion or failure.
                self.logger.info(
                    "Resuming incomplete job {} with external jobid '{}'.".format(
                        job.jobid, ext_jobid
                    )
                )
                self.external_jobid.update((f, ext_jobid) for f in job.output)
                self.report_job_submission(
                    SubmittedJobInfo(job, external_jobid=ext_jobid)
                )
                return

        deps = " ".join(
            self.external_jobid[f] for f in job.input if f in self.external_jobid
        )
        try:
            submitcmd = job.format_wildcards(
                self.workflow.executor_settings.submit_cmd, dependencies=deps
            )
        except AttributeError as e:
            raise WorkflowError(str(e), rule=job.rule if not job.is_group() else None)

        try:
            env = dict(os.environ)
            if self.sidecar_vars:
                env["SNAKEMAKE_CLUSTER_SIDECAR_VARS"] = self.sidecar_vars

            # Remove SNAKEMAKE_PROFILE from environment as the snakemake call inside
            # of the cluster job must run locally (or complains about missing -j).
            env.pop("SNAKEMAKE_PROFILE", None)

            ext_jobid = (
                subprocess.check_output(
                    '{submitcmd} "{jobscript}"'.format(
                        submitcmd=submitcmd, jobscript=jobscript
                    ),
                    shell=True,
                    env=env,
                )
                .decode()
                .split("\n")
            )
        except subprocess.CalledProcessError as ex:
            msg = "Error submitting jobscript (exit code {}):\n{}".format(
                ex.returncode, ex.output.decode()
            )
            self.report_job_error(job_info, msg=msg)
            return

        if ext_jobid and ext_jobid[0]:
            ext_jobid = ext_jobid[0].strip()
            job_info.external_jobid = ext_jobid

            self.external_jobid.update((f, ext_jobid) for f in job.output)

            self.logger.info(
                "Submitted {} {} with external jobid '{}'.".format(
                    "group job" if job.is_group() else "job", job.jobid, ext_jobid
                )
            )

        self.report_job_submission(job_info)

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        success = "success"
        failed = "failed"
        running = "running"

        if self.workflow.executor_settings.status_cmd is not None:

            def job_status(
                job_info: SubmittedJobInfo,
                valid_returns=["running", "success", "failed"],
            ):
                try:
                    # this command shall return "success", "failed" or "running"
                    env = dict(os.environ)
                    if self.sidecar_vars:
                        env["SNAKEMAKE_CLUSTER_SIDECAR_VARS"] = self.sidecar_vars
                    ret = subprocess.check_output(
                        "{statuscmd} '{jobid}'".format(
                            jobid=job_info.external_jobid,
                            statuscmd=self.workflow.executor_settings.status_cmd,
                        ),
                        shell=True,
                        env=env,
                    ).decode()
                except subprocess.CalledProcessError as e:
                    if e.returncode < 0:
                        # Ignore SIGINT and all other issues due to signals
                        # because it will be caused by hitting e.g.
                        # Ctrl-C on the main process or sending killall to
                        # snakemake.
                        # Snakemake will handle the signal in
                        # the main process.
                        self.status_cmd_kills.append(-e.returncode)
                        if len(self.status_cmd_kills) > 10:
                            self.logger.info(
                                "Cluster status command {} was killed >10 "
                                "times with signal(s) {} "
                                "(if this happens unexpectedly during your "
                                "workflow execution, "
                                "have a closer look.).".format(
                                    self.statuscmd, ",".join(self.status_cmd_kills)
                                )
                            )
                            self.status_cmd_kills.clear()
                    else:
                        raise WorkflowError(
                            "Failed to obtain job status. "
                            "See above for error message."
                        )

                ret = ret.strip().split("\n")
                if len(ret) != 1 or ret[0] not in valid_returns:
                    raise WorkflowError(
                        "Cluster status command {} returned {} but just a single "
                        "line with one of {} is expected.".format(
                            self.statuscmd, "\\n".join(ret), ",".join(valid_returns)
                        )
                    )
                return ret[0]

        else:

            def job_status(job_info: SubmittedJobInfo):
                jobfinished = job_info.aux["jobfinished"]
                jobfailed = job_info.aux["jobfailed"]
                jobscript = job_info.aux["jobscript"]
                if os.path.exists(jobfinished):
                    os.remove(jobfinished)
                    os.remove(jobscript)
                    return success
                if os.path.exists(jobfailed):
                    os.remove(jobfailed)
                    os.remove(jobscript)
                    return failed
                return running

        for active_job in active_jobs:
            async with self.status_rate_limiter:
                status = job_status(active_job)

                if status == success:
                    self.report_job_success(active_job)
                elif status == failed:
                    self.report_job_error(active_job)
                else:
                    # still active, yield again
                    yield active_job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        def _chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        if self.cancelcmd:  # We have --cluster-cancel
            # Enumerate job IDs and create chunks.
            # If cancelnargs evaluates to false (0/None)
            # then pass all job ids at once
            jobids = [job_info.external_jobid for job_info in active_jobs]
            chunks = list(_chunks(jobids, self.cancelnargs or len(jobids)))
            # Go through the chunks and cancel the jobs, warn in case of failures.
            failures = 0
            for chunk in chunks:
                try:
                    # rather fail on timeout than miss canceling all
                    cancel_timeout = 2
                    env = dict(os.environ)
                    if self.sidecar_vars:
                        env["SNAKEMAKE_CLUSTER_SIDECAR_VARS"] = self.sidecar_vars
                    subprocess.check_call(
                        [self.cancelcmd] + chunk,
                        shell=False,
                        timeout=cancel_timeout,
                        env=env,
                    )
                except subprocess.SubprocessError:
                    failures += 1
            if failures:
                self.logger.info(
                    (
                        "{} out of {} calls to --cluster-cancel failed. "
                        "This is safe to "
                        "ignore in most cases."
                    ).format(failures, len(chunks))
                )
        else:
            self.logger.info(
                "No --cluster-cancel given. Will exit after "
                "finishing currently running jobs."
            )
            self.shutdown()

    def get_job_exec_prefix(self, job: JobExecutorInterface):
        if self.workflow.storage_settings.assume_shared_fs:
            return f"cd {shlex.quote(self.workflow.workdir_init)}"
        else:
            return ""

    def get_job_exec_suffix(self, job: JobExecutorInterface):
        if self.workflow.executor_settings.status_cmd:
            return "exit 0 || exit 1"
        elif self.workflow.storage_settings.assume_shared_fs:
            # TODO wrap with watch and touch {jobrunning}
            # check modification date of {jobrunning} in the wait_for_job method

            return (
                f"touch {repr(self.get_jobfinished_marker(job))} || "
                f"(touch {repr(self.get_jobfailed_marker(job))}; exit 1)"
            )
        assert False, "bug: neither statuscmd defined nor shared FS"

    def get_jobfinished_marker(self, job: JobExecutorInterface):
        return os.path.join(self.tmpdir, f"{job.jobid}.jobfinished")

    def get_jobfailed_marker(self, job: JobExecutorInterface):
        return os.path.join(self.tmpdir, f"{job.jobid}.jobfailed")

    def _launch_sidecar(self):
        def copy_stdout(executor, process):
            """Run sidecar process and copy it's stdout to our stdout."""
            while process.poll() is None and executor.wait:
                buf = process.stdout.readline()
                if buf:
                    sys.stdout.write(buf)
            # one final time ...
            buf = process.stdout.readline()
            if buf:
                sys.stdout.write(buf)

        def wait(executor, process):
            while executor.wait:
                time.sleep(0.5)
            process.terminate()
            process.wait()
            self.logger.info(
                "Cluster sidecar process has terminated (retcode=%d)."
                % process.returncode
            )

        self.logger.info("Launch sidecar process and read first output line.")
        process = subprocess.Popen(
            self.workflow.executor_settings.sidecar_cmd,
            stdout=subprocess.PIPE,
            shell=False,
            encoding="utf-8",
        )
        self.sidecar_vars = process.stdout.readline()
        while self.sidecar_vars and self.sidecar_vars[-1] in "\n\r":
            self.sidecar_vars = self.sidecar_vars[:-1]
        self.logger.info("Done reading first output line.")

        thread_stdout = threading.Thread(
            target=copy_stdout, name="sidecar_stdout", args=(self, process)
        )
        thread_stdout.start()
        thread_wait = threading.Thread(
            target=wait, name="sidecar_stdout", args=(self, process)
        )
        thread_wait.start()
