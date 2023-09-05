from pathlib import Path
from typing import Optional
from snakemake.common.tests import TestWorkflowsBase
from snakemake_executor_plugin_cluster_generic import ExecutorSettings
from snakemake_interface_executor_plugins import ExecutorSettingsBase


class TestWorkflows(TestWorkflowsBase):
    def get_executor(self) -> str:
        return "cluster-generic"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        qsub = Path(__file__).parent / "qsub.sh"
        return ExecutorSettings(submit_cmd=str(qsub.absolute()))

    def get_default_remote_provider(self) -> Optional[str]:
        return None

    def get_default_remote_prefix(self) -> Optional[str]:
        return None

    def test_simple_workflow(self, tmp_path):
        super().test_simple_workflow(tmp_path)