
# OPEN BATCH AND GO_NO_GO
~/thea/scripts/run_thea_job.sh JOB_D9318_GORWCA_D_OPEN_BATCH &&
~/thea/scripts/run_thea_job.sh JOB_D9318_GORWCA_D_GO_NO_GO

# PARTITION KEY JOB
~/thea/scripts/run_thea_job.sh JOB_PARTITION_KEY_BGORWCA_D


# MAIN JOBS
~/thea/scripts/run_thea_job.sh JOB_TXF_CDH345_GOREWARDS_CREDIT_ACCRUAL_D9318_T01_D
~/thea/scripts/run_thea_job.sh JOB_RCN_CDH345_GOREWARDS_CREDIT_ACCRUAL_D9318_W01_D
~/thea/scripts/run_thea_job.sh JOB_EXP_CDH345_GOREWARDS_CREDIT_ACCRUAL_D9318_T01_D


# CLOSE BATCH
~/thea/scripts/run_thea_job.sh JOB_D9318_GORWCA_D_CLOSE_BATCH










