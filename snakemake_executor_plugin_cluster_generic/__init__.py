from dataclasses import dataclass, field
import os
import shlex
import subprocess
from typing import List, Set

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins import ExecutorSettingsBase, CommonSettings
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
from snakemake_interface_executor_plugins.jobs import (
    ExecutorJobInterface,
)


# Optional:
# define additional settings for your executor
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    submit_cmd: str = field(metadata={"help": "Command for submitting jobs"})
    status_cmd: str = field(metadata={"help": "Command for retrieving job status"})
    cancel_cmd: str = field(
        metadata={
            "help": "Command for cancelling jobs. Expected to take one or more jobids as arguments."
        }
    )
    cancel_nargs: int = field(
        metadata={
            "help": "Number of jobids to pass to cancel_cmd. If more are given, cancel_cmd will be called multiple times."
        }
    )
    sidecarcmd: str = field(metadata={"help": "Command for sidecar process."})


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True
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
            pass_default_remote_provider_args=True,  # whether arguments for setting the remote provider shall  be passed to jobs
            pass_default_resources_args=True,  # whether arguments for setting default resources shall be passed to jobs
            pass_envvar_declarations_to_cmd=True,  # whether environment variables shall be passed to jobs
        )

        self.sidecar_vars = None
        if self.workflow.executor_settings.sidecar_cmd:
            self._launch_sidecar()

        if (
            not self.workflow.executor_settings.status_cmd
            and not self.workflow.assume_shared_fs
        ):
            raise WorkflowError(
                "If no shared filesystem is used, you have to "
                "specify a cluster status command."
            )

        self.status_cmd_kills = []

    def get_job_exec_prefix(self, job: ExecutorJobInterface):
        if self.assume_shared_fs:
            return f"cd {shlex.quote(self.workflow.workdir_init)}"
        else:
            return ""

    def run_job(self, job: ExecutorJobInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call self.report_job_submission(job_info).
        # with job_info being of type snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

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
                self.report_job_submission(
                    SubmittedJobInfo(job, external_jobid=ext_jobid)
                )
                return

        deps = " ".join(
            self.external_jobid[f] for f in job.input if f in self.external_jobid
        )
        try:
            submitcmd = job.format_wildcards(self.submitcmd, dependencies=deps)
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
            self.logger.error(
                "Error submitting jobscript (exit code {}):\n{}".format(
                    ex.returncode, ex.output.decode()
                )
            )
            self.report_job_error(job)
            return

        if ext_jobid and ext_jobid[0]:
            job_info.external_jobid = ext_jobid[0]

            self.logger.info(
                "Submitted {} {} with external jobid '{}'.".format(
                    "group job" if job.is_group() else "job", job.jobid, ext_jobid
                )
            )

        self.report_job_submission(job_info)

    def check_active_jobs(self, active_jobs: Set[SubmittedJobInfo]):
        # Check the status of active jobs.

        # Jobs that are finished or error out have to be removed from the given list.
        # For jobs that have finished successfully, you have to call self.report_job_success(job).
        # For jobs that have errored, you have to call self.report_job_error(job).

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
                                "Cluster status command {} was killed >10 times with signal(s) {} "
                                "(if this happens unexpectedly during your workflow execution, "
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
                        "Cluster status command {} returned {} but just a single line with one of {} is expected.".format(
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

        for active_job in list(active_jobs):
            with self.status_rate_limiter:
                status = job_status(active_job)

                if status == success:
                    self.report_job_success(active_job.job)
                    active_jobs.remove(active_job)
                elif status == failed:
                    self.print_job_error(
                        active_job.job,
                        cluster_jobid=active_job.jobid
                        if active_job.jobid
                        else "unknown",
                    )
                    self.print_cluster_job_error(
                        active_job, self.dag.jobid(active_job.job)
                    )
                    self.report_job_error(active_job.job)
                    active_jobs.remove(active_job)

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        def _chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        if self.cancelcmd:  # We have --cluster-cancel
            # Enumerate job IDs and create chunks.  If cancelnargs evaluates to false (0/None)
            # then pass all job ids at once
            jobids = [job_info.aux["external_jobid"] for job_info in active_jobs]
            chunks = list(_chunks(jobids, self.cancelnargs or len(jobids)))
            # Go through the chunks and cancel the jobs, warn in case of failures.
            failures = 0
            for chunk in chunks:
                try:
                    cancel_timeout = 2  # rather fail on timeout than miss canceling all
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
                        "{} out of {} calls to --cluster-cancel failed.  This is safe to "
                        "ignore in most cases."
                    ).format(failures, len(chunks))
                )
        else:
            self.logger.info(
                "No --cluster-cancel given. Will exit after finishing currently running jobs."
            )
            self.shutdown()
