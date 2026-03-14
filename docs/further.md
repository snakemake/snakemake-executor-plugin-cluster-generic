### Example setup

The cluster generic plugin enables access to workflow variables (such as `rule`, `resources`, or `threads`) within the strings used to define cluster generic plugin arguments.
This makes it possible to create custom configurations for running workflows on various cluster systems that support a submission command accepting the path to a job script. The following example Snakemake profile (see [Profiles](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles)) shows this for a custom SLURM execution configuration setup. Submitting jobs with this setup would result in having the rule name of the current job contained in the SLURM job name.

```yaml
executor: cluster-generic
cluster-generic-submit-cmd: sbatch --cpus-per-task {threads} --mem={resources.mem_mb} --job-name=smk-{rule} --parsable
cluster-generic-cancel-cmd: scancel
```

