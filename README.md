# DQM - Data Quality Management Framework

DQM is a Python library designed to help manage and ensure data quality in data pipelines. It provides functionalities for data validation, alerting, and seamless integration with Spark.

## Installation

Use the package manager [pip]() with git to install DQM.

```bash
pip install git+https://git_user_name:git_token@github.com/Maersk-Global/ALP_DQ.git
```

## Usage

```python
# importing DQM package
import DQM as dq

#Initializing DQM package 
out=run_DQM(file_path='https://raw.githubusercontent.com/Org Name/repo/branch/[folder]/dq_config.json',
    github_user='git username',
    github_pao=' git token', 
    QC_table='', # /mnt/commbi_pub_transportmgmt/saptm_qc_poc
    table_base_path ='', #/mnt/commbi_pub_transportmgmt/raw/
    format='delta',
    read_type='full',
    alert_on='y',
    config_file='logic app url which use to send alerts')
    spark.read.format('orc').option("mergeSchema", "true").load(out.table_info[0]).\
    write.mode('append').save(out.table_info[1])
    dbutils.fs.rm(out.table_info[0],True)
```

## Parameters
* **file_path:** URL to the DQ configuration JSON file which is created by DQ configuration tool.

* **github_user:** GitHub username for accessing the configuration repository.

* **github_pao:** GitHub token for accessing the configuration repository.

* **QC_table:** Quality control table path (optional).

* **table_base_path:** Base path for the raw data tables (optional).

* **format:** Format of the data files (e.g., 'delta', 'parquet', 'orc').

* **read_type:** Type of read operation ('full' or 'incremental').

* **alert_on:** Flag to enable or disable alerts ('y' for yes, 'n' for no).

* **config_file:** URL to the Logic App configuration for sending alerts.

## Example Workflow
* **Initialize the DQM package:** Set up the necessary parameters and configuration file to define the data quality rules and alert mechanisms.

* **Read data using Spark:** Load the data from the specified source and format using Spark.

* **Perform data quality operations:** Validate and transform the data based on the predefined rules.

* **Write data:** Save the cleaned and validated data back to the specified destination.

* **Clean up:** Remove temporary files and resources to maintain a clean environment.

## Contributing

We welcome contributions to improve DQM. If you would like to contribute, please follow these steps:

1.  Fork the repository.
    
2.  Create a new branch (git checkout -b feature-branch).
    
3.  Make your changes.
    
4.  Commit your changes (git commit -m 'Add new feature').
    
5.  Push to the branch (git push origin feature-branch).
    
6.  Open a pull request.
    

For major changes, please open an issue first to discuss what you would like to change.

Ensure that you update tests as appropriate when contributing.

## Contact

For any issues or inquiries, please contact us at:

*   [santosh.birje@maersk.com](santosh.birje@maersk.com)
    
*   [ipsa.panda@maersk.com](ipsa.panda@maersk.com)
    

We appreciate your feedback and contributions to make DQM better!
