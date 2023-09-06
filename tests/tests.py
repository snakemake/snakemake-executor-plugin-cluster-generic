__author__ = "Johannes Köster, Manuel Holtgrewe"
__copyright__ = "Copyright 2023, Johannes Köster, Manuel Holtgrewe"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from pathlib import Path
from typing import Optional
import snakemake.common.tests
from snakemake_executor_plugin_cluster_generic import ExecutorSettings
from snakemake_interface_executor_plugins import ExecutorSettingsBase
from snakemake_interface_common.exceptions import WorkflowError


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsBase):
    def get_executor(self) -> str:
        return "cluster-generic"

    def _get_cmd(self, cmd) -> str:
        return str((Path(__file__).parent / cmd).absolute())

    def _get_executor_settings(self, **kwargs) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings(submit_cmd=self._get_cmd("qsub.sh"), **kwargs)

    def get_default_remote_provider(self) -> Optional[str]:
        return None

    def get_default_remote_prefix(self) -> Optional[str]:
        return None


class TestWorkflowsNoSubmitCmd(TestWorkflowsBase):
    __test__ = True
    expect_exception = WorkflowError

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        settings = self._get_executor_settings()
        settings.submit_cmd = None
        return settings


class TestWorkflowsNoStatusCmdNoSharedFs(TestWorkflowsBase):
    __test__ = True
    expect_exception = WorkflowError

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return self._get_executor_settings(status_cmd=self._get_cmd("qstatus.sh"))

    def get_assume_shared_fs(self) -> bool:
        return False


class TestWorkflowsSubmitCmdOnly(TestWorkflowsBase):
    __test__ = True

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return self._get_executor_settings()


class TestWorkflowsCancelCmd(TestWorkflowsBase):
    __test__ = True

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return self._get_executor_settings(cancel_cmd=self._get_cmd("qdel.sh"))


class TestWorkflowsStatusCmd(TestWorkflowsBase):
    __test__ = True

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return self._get_executor_settings(status_cmd=self._get_cmd("qstatus.sh"))


class TestWorkflowsSidecar(TestWorkflowsBase):
    __test__ = True

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return self._get_executor_settings(sidecar_cmd=self._get_cmd("sidecar.sh"))
